#include "execution/compiler/operator/sort_translator.h"

#include <utility>

#include "execution/compiler/compilation_context.h"
#include "execution/compiler/function_builder.h"
#include "execution/compiler/if.h"
#include "execution/compiler/loop.h"
#include "execution/compiler/work_context.h"
#include "execution/sql/sorter.h"
#include "planner/plannodes/order_by_plan_node.h"
#include "planner/plannodes/output_schema.h"

namespace noisepage::execution::compiler {

namespace {
constexpr const char SORT_ROW_ATTR_PREFIX[] = "attr";
}  // namespace

SortTranslator::SortTranslator(const planner::OrderByPlanNode &plan, CompilationContext *compilation_context,
                               Pipeline *pipeline)
    : OperatorTranslator(plan, compilation_context, pipeline, brain::ExecutionOperatingUnitType::DUMMY),
      sort_row_var_(GetCodeGen()->MakeFreshIdentifier("sortRow")),
      sort_row_type_(GetCodeGen()->MakeFreshIdentifier("SortRow")),
      lhs_row_(GetCodeGen()->MakeIdentifier("lhs")),
      rhs_row_(GetCodeGen()->MakeIdentifier("rhs")),
      compare_func_(GetCodeGen()->MakeFreshIdentifier(pipeline->CreatePipelineFunctionName("Compare"))),
      build_pipeline_(this, Pipeline::Parallelism::Parallel),
      current_row_(CurrentRow::Child) {
  NOISEPAGE_ASSERT(plan.GetChildrenSize() == 1, "Sorts expected to have a single child.");
  // Register this as the source for the pipeline. It must be serial to maintain
  // sorted output order.
  pipeline->RegisterSource(this, Pipeline::Parallelism::Serial);

  // The build pipeline must complete before the produce pipeline.
  pipeline->LinkSourcePipeline(&build_pipeline_);

  // Prepare the child.
  compilation_context->Prepare(*plan.GetChild(0), &build_pipeline_);

  // Prepare the sort-key expressions.
  for (const auto &[expr, _] : plan.GetSortKeys()) {
    (void)_;
    compilation_context->Prepare(*expr);
  }

  // Register a Sorter instance in the global query state.
  CodeGen *codegen = compilation_context->GetCodeGen();
  ast::Expr *sorter_type = codegen->BuiltinType(ast::BuiltinType::Sorter);
  global_sorter_ = compilation_context->GetQueryState()->DeclareStateEntry(codegen, "sorter", sorter_type);

  // Register another Sorter instance in the pipeline-local state if the
  // build pipeline is parallel.
  if (build_pipeline_.IsParallel()) {
    local_sorter_ = build_pipeline_.DeclarePipelineStateEntry("sorter", sorter_type);
  }

  num_sort_build_rows_ = CounterDeclare("num_sort_build_rows", &build_pipeline_);
  num_sort_iterate_rows_ = CounterDeclare("num_sort_iterate_rows", pipeline);

  if (build_pipeline_.IsParallel() && IsPipelineMetricsEnabled()) {
    parallel_starttlsort_hook_fn_ =
        GetCodeGen()->MakeFreshIdentifier(build_pipeline_.CreatePipelineFunctionName("StartTLSortHook"));
    parallel_starttlmerge_hook_fn_ =
        GetCodeGen()->MakeFreshIdentifier(build_pipeline_.CreatePipelineFunctionName("StartTLMergeHook"));
    parallel_endtlsort_hook_fn_ =
        GetCodeGen()->MakeFreshIdentifier(build_pipeline_.CreatePipelineFunctionName("EndTLSortHook"));
    parallel_endtlmerge_hook_fn_ =
        GetCodeGen()->MakeFreshIdentifier(build_pipeline_.CreatePipelineFunctionName("EndTLMergeHook"));
    parallel_endsinglesorter_hook_fn_ =
        GetCodeGen()->MakeFreshIdentifier(build_pipeline_.CreatePipelineFunctionName("EndSingleSorterHook"));
  }
}

void SortTranslator::DefineHelperStructs(util::RegionVector<ast::StructDecl *> *decls) {
  auto *codegen = GetCodeGen();
  auto fields = codegen->MakeEmptyFieldList();
  GetAllChildOutputFields(0, SORT_ROW_ATTR_PREFIX, &fields);
  ast::StructDecl *struct_decl = codegen->DeclareStruct(sort_row_type_, std::move(fields));
  struct_decl_ = struct_decl;
  decls->push_back(struct_decl);
}

void SortTranslator::GenerateComparisonFunction(FunctionBuilder *function) {
  auto *codegen = GetCodeGen();
  WorkContext context(GetCompilationContext(), build_pipeline_);
  context.SetExpressionCacheEnable(false);
  int32_t ret_value;
  for (const auto &[expr, sort_order] : GetPlanAs<planner::OrderByPlanNode>().GetSortKeys()) {
    if (sort_order == optimizer::OrderByOrderingType::ASC) {
      ret_value = -1;
    } else {
      ret_value = 1;
    }
    for (const auto tok : {parsing::Token::Type::LESS, parsing::Token::Type::GREATER}) {
      current_row_ = CurrentRow::Lhs;
      ast::Expr *lhs = context.DeriveValue(*expr, this);
      current_row_ = CurrentRow::Rhs;
      ast::Expr *rhs = context.DeriveValue(*expr, this);
      If check_comparison(function, codegen->Compare(tok, lhs, rhs));
      {
        // Return the appropriate value based on ordering.
        function->Append(codegen->Return(codegen->Const32(ret_value)));
      }
      check_comparison.EndIf();
      ret_value = -ret_value;
    }
  }
  current_row_ = CurrentRow::Child;
}

void SortTranslator::DefineHelperFunctions(util::RegionVector<ast::FunctionDecl *> *decls) {
  auto *codegen = GetCodeGen();
  auto params = codegen->MakeFieldList({
      codegen->MakeField(lhs_row_, codegen->PointerType(sort_row_type_)),
      codegen->MakeField(rhs_row_, codegen->PointerType(sort_row_type_)),
  });
  FunctionBuilder builder(codegen, compare_func_, std::move(params), codegen->Int32Type());
  {
    // Generate body.
    GenerateComparisonFunction(&builder);
  }
  decls->push_back(builder.Finish(codegen->Const32(0)));
}

void SortTranslator::DefineTLSDependentHelperFunctions(const Pipeline &pipeline,
                                                       util::RegionVector<ast::FunctionDecl *> *decls) {
  if (IsBuildPipeline(pipeline) && build_pipeline_.IsParallel() && IsPipelineMetricsEnabled()) {
    decls->push_back(GenerateStartTLHookFunction(true));
    decls->push_back(GenerateStartTLHookFunction(false));
    decls->push_back(GenerateEndTLSortHookFunction());
    decls->push_back(GenerateEndTLMergeHookFunction());
    decls->push_back(GenerateEndSingleSorterHookFunction());
  }
}

void SortTranslator::InitializeSorter(FunctionBuilder *function, ast::Expr *sorter_ptr) const {
  auto ctx = GetExecutionContext();
  function->Append(GetCodeGen()->SorterInit(sorter_ptr, ctx, compare_func_, sort_row_type_));
}

void SortTranslator::TearDownSorter(FunctionBuilder *function, ast::Expr *sorter_ptr) const {
  function->Append(GetCodeGen()->SorterFree(sorter_ptr));
}

void SortTranslator::InitializeQueryState(FunctionBuilder *function) const {
  InitializeSorter(function, global_sorter_.GetPtr(GetCodeGen()));
}

void SortTranslator::TearDownQueryState(FunctionBuilder *function) const {
  TearDownSorter(function, global_sorter_.GetPtr(GetCodeGen()));
}

ast::FunctionDecl *SortTranslator::GenerateStartTLHookFunction(bool is_sort) const {
  auto *codegen = GetCodeGen();
  auto *pipeline = &build_pipeline_;

  auto name = parallel_starttlsort_hook_fn_;
  auto filter = brain::ExecutionOperatingUnitType::PARALLEL_SORT_STEP;
  if (!is_sort) {
    name = parallel_starttlmerge_hook_fn_;
    filter = brain::ExecutionOperatingUnitType::PARALLEL_SORT_MERGE_STEP;
  }

  auto params = GetHookParams(*pipeline, nullptr, nullptr);
  auto ret_type = codegen->BuiltinType(ast::BuiltinType::Kind::Nil);
  FunctionBuilder builder(codegen, name, std::move(params), ret_type);
  {
    pipeline->InjectStartResourceTracker(&builder, true);
    builder.Append(
        codegen->CallBuiltin(ast::Builtin::ExecOUFeatureVectorFilter,
                             {pipeline->OUFeatureVecPtr(), codegen->Const32(static_cast<uint32_t>(filter))}));
  }
  return builder.Finish();
}

ast::FunctionDecl *SortTranslator::GenerateEndTLSortHookFunction() const {
  auto *codegen = GetCodeGen();
  auto *pipeline = &build_pipeline_;
  auto params = GetHookParams(*pipeline, nullptr, nullptr);

  auto ret_type = codegen->BuiltinType(ast::BuiltinType::Kind::Nil);
  FunctionBuilder builder(codegen, parallel_endtlsort_hook_fn_, std::move(params), ret_type);
  {
    auto num_tuples = codegen->MakeFreshIdentifier("num_tuples");
    auto *sorter_size = codegen->CallBuiltin(ast::Builtin::SorterGetTupleCount, {local_sorter_.GetPtr(codegen)});
    builder.Append(codegen->DeclareVarWithInit(num_tuples, sorter_size));

    // FeatureRecord with the overrideValue
    FeatureRecord(&builder, brain::ExecutionOperatingUnitType::PARALLEL_SORT_STEP,
                  brain::ExecutionOperatingUnitFeatureAttribute::NUM_ROWS, *pipeline, codegen->MakeExpr(num_tuples));
    FeatureRecord(&builder, brain::ExecutionOperatingUnitType::PARALLEL_SORT_STEP,
                  brain::ExecutionOperatingUnitFeatureAttribute::CARDINALITY, *pipeline, codegen->MakeExpr(num_tuples));

    // End Tracker
    pipeline->InjectEndResourceTracker(&builder, true);
  }
  return builder.Finish();
}

ast::FunctionDecl *SortTranslator::GenerateEndTLMergeHookFunction() const {
  auto *codegen = GetCodeGen();
  auto *pipeline = &build_pipeline_;

  auto override_value = codegen->MakeIdentifier("overrideValue");
  auto int32_type = codegen->BuiltinType(ast::BuiltinType::Uint32);
  auto params = GetHookParams(*pipeline, &override_value, int32_type);

  auto ret_type = codegen->BuiltinType(ast::BuiltinType::Kind::Nil);
  FunctionBuilder builder(codegen, parallel_endtlmerge_hook_fn_, std::move(params), ret_type);
  {
    // FeatureRecord with the overrideValue
    FeatureRecord(&builder, brain::ExecutionOperatingUnitType::PARALLEL_SORT_MERGE_STEP,
                  brain::ExecutionOperatingUnitFeatureAttribute::NUM_ROWS, *pipeline,
                  codegen->MakeExpr(override_value));
    FeatureRecord(&builder, brain::ExecutionOperatingUnitType::PARALLEL_SORT_MERGE_STEP,
                  brain::ExecutionOperatingUnitFeatureAttribute::CARDINALITY, *pipeline,
                  codegen->MakeExpr(override_value));

    // End Tracker
    pipeline->InjectEndResourceTracker(&builder, true);
  }
  return builder.Finish();
}

ast::FunctionDecl *SortTranslator::GenerateEndSingleSorterHookFunction() const {
  auto *codegen = GetCodeGen();
  auto *pipeline = &build_pipeline_;

  auto sorter = codegen->MakeIdentifier("sorter");
  auto sorter_type = codegen->PointerType(codegen->BuiltinType(ast::BuiltinType::Sorter));
  auto params = GetHookParams(*pipeline, &sorter, sorter_type);

  auto ret_type = codegen->BuiltinType(ast::BuiltinType::Kind::Nil);
  FunctionBuilder builder(codegen, parallel_endsinglesorter_hook_fn_, std::move(params), ret_type);
  {
    auto num_tuples = codegen->MakeFreshIdentifier("num_tuples");
    auto *sorter_size = codegen->CallBuiltin(ast::Builtin::SorterGetTupleCount, {codegen->MakeExpr(sorter)});
    builder.Append(codegen->DeclareVarWithInit(num_tuples, sorter_size));

    // FeatureRecord with the overrideValue
    FeatureRecord(&builder, brain::ExecutionOperatingUnitType::PARALLEL_SORT_STEP,
                  brain::ExecutionOperatingUnitFeatureAttribute::NUM_ROWS, *pipeline, codegen->MakeExpr(num_tuples));
    FeatureRecord(&builder, brain::ExecutionOperatingUnitType::PARALLEL_SORT_STEP,
                  brain::ExecutionOperatingUnitFeatureAttribute::CARDINALITY, *pipeline, codegen->MakeExpr(num_tuples));

    // End Tracker
    pipeline->InjectEndResourceTracker(&builder, true);
  }
  return builder.Finish();
}

void SortTranslator::InitializePipelineState(const Pipeline &pipeline, FunctionBuilder *function) const {
  if (IsBuildPipeline(pipeline) && build_pipeline_.IsParallel()) {
    InitializeSorter(function, local_sorter_.GetPtr(GetCodeGen()));
  }

  InitializeCounters(pipeline, function);
}

void SortTranslator::InitializeCounters(const Pipeline &pipeline, FunctionBuilder *function) const {
  if (IsBuildPipeline(pipeline)) {
    CounterSet(function, num_sort_build_rows_, 0);
  } else {
    CounterSet(function, num_sort_iterate_rows_, 0);
  }
}

void SortTranslator::RecordCounters(const Pipeline &pipeline, FunctionBuilder *function) const {
  auto *codegen = GetCodeGen();
  if (IsBuildPipeline(pipeline)) {
    FeatureRecord(function, brain::ExecutionOperatingUnitType::SORT_BUILD,
                  brain::ExecutionOperatingUnitFeatureAttribute::NUM_ROWS, pipeline, CounterVal(num_sort_build_rows_));
    FeatureRecord(function, brain::ExecutionOperatingUnitType::SORT_BUILD,
                  brain::ExecutionOperatingUnitFeatureAttribute::CARDINALITY, pipeline,
                  CounterVal(num_sort_build_rows_));
    FeatureArithmeticRecordMul(function, pipeline, GetTranslatorId(), CounterVal(num_sort_build_rows_));
  } else {
    ast::Expr *sorter_ptr = global_sorter_.GetPtr(codegen);
    FeatureRecord(function, brain::ExecutionOperatingUnitType::SORT_ITERATE,
                  brain::ExecutionOperatingUnitFeatureAttribute::NUM_ROWS, pipeline,
                  CounterVal(num_sort_iterate_rows_));
    FeatureRecord(function, brain::ExecutionOperatingUnitType::SORT_ITERATE,
                  brain::ExecutionOperatingUnitFeatureAttribute::CARDINALITY, pipeline,
                  codegen->CallBuiltin(ast::Builtin::SorterGetTupleCount, {sorter_ptr}));
    FeatureArithmeticRecordMul(function, pipeline, GetTranslatorId(), CounterVal(num_sort_iterate_rows_));
  }
}

void SortTranslator::EndParallelPipelineWork(const Pipeline &pipeline, FunctionBuilder *function) const {
  RecordCounters(pipeline, function);
}

void SortTranslator::TearDownPipelineState(const Pipeline &pipeline, FunctionBuilder *function) const {
  auto *codegen = GetCodeGen();
  if (IsBuildPipeline(pipeline) && pipeline.IsParallel()) {
    ast::Expr *sorter_ptr = local_sorter_.GetPtr(codegen);
    TearDownSorter(function, sorter_ptr);
  }
}

ast::Expr *SortTranslator::GetSortRowAttribute(ast::Identifier sort_row, uint32_t attr_idx) const {
  auto *codegen = GetCodeGen();
  ast::Identifier attr_name = codegen->MakeIdentifier(SORT_ROW_ATTR_PREFIX + std::to_string(attr_idx));
  return codegen->AccessStructMember(codegen->MakeExpr(sort_row), attr_name);
}

void SortTranslator::FillSortRow(WorkContext *ctx, FunctionBuilder *function) const {
  auto *codegen = GetCodeGen();
  const auto child_schema = GetPlan().GetChild(0)->GetOutputSchema();
  for (uint32_t attr_idx = 0; attr_idx < child_schema->GetColumns().size(); attr_idx++) {
    ast::Expr *lhs = GetSortRowAttribute(sort_row_var_, attr_idx);
    ast::Expr *rhs = GetChildOutput(ctx, 0, attr_idx);
    function->Append(codegen->Assign(lhs, rhs));
  }
}

void SortTranslator::InsertIntoSorter(WorkContext *ctx, FunctionBuilder *function) const {
  auto *codegen = GetCodeGen();

  // Collect correct sorter instance.
  const auto sorter = ctx->GetPipeline().IsParallel() ? local_sorter_ : global_sorter_;
  ast::Expr *sorter_ptr = sorter.GetPtr(codegen);

  if (const auto &plan = GetPlanAs<planner::OrderByPlanNode>(); plan.HasLimit()) {
    const std::size_t top_k = plan.GetOffset() + plan.GetLimit();
    ast::Expr *insert_call = codegen->SorterInsertTopK(sorter_ptr, sort_row_type_, top_k);
    function->Append(codegen->DeclareVarWithInit(sort_row_var_, insert_call));
    FillSortRow(ctx, function);
    function->Append(codegen->SorterInsertTopKFinish(sorter.GetPtr(codegen), top_k));
  } else {
    ast::Expr *insert_call = codegen->SorterInsert(sorter_ptr, sort_row_type_);
    function->Append(codegen->DeclareVarWithInit(sort_row_var_, insert_call));
    FillSortRow(ctx, function);
  }
}

void SortTranslator::ScanSorter(WorkContext *ctx, FunctionBuilder *function) const {
  auto *codegen = GetCodeGen();

  // var sorter_base: Sorter
  auto base_iter_name = codegen->MakeFreshIdentifier("iterBase");
  function->Append(codegen->DeclareVarNoInit(base_iter_name, ast::BuiltinType::SorterIterator));

  // var sorter = &sorter_base
  auto iter_name = codegen->MakeFreshIdentifier("iter");
  auto iter = codegen->MakeExpr(iter_name);
  function->Append(codegen->DeclareVarWithInit(iter_name, codegen->AddressOf(codegen->MakeExpr(base_iter_name))));

  // Call @sorterIterInit().
  function->Append(codegen->SorterIterInit(iter, global_sorter_.GetPtr(codegen)));

  ast::Expr *init = nullptr;
  if (const auto offset = GetPlanAs<planner::OrderByPlanNode>().GetOffset(); offset != 0) {
    init = codegen->SorterIterSkipRows(iter, offset);
  }
  Loop loop(function,
            init == nullptr ? nullptr : codegen->MakeStmt(init),  // @sorterIterSkipRows();
            codegen->SorterIterHasNext(iter),                     // @sorterIterHasNext();
            codegen->MakeStmt(codegen->SorterIterNext(iter)));    // @sorterIterNext()
  {
    // var sortRow = @ptrCast(SortRow*, @sorterIterGetRow(sorter))
    auto row = codegen->SorterIterGetRow(iter, sort_row_type_);
    function->Append(codegen->DeclareVarWithInit(sort_row_var_, row));
    // Move along
    ctx->Push(function);

    CounterAdd(function, num_sort_iterate_rows_, 1);
  }
  loop.EndLoop();

  // @sorterIterClose()
  function->Append(codegen->SorterIterClose(iter));
}

void SortTranslator::PerformPipelineWork(WorkContext *ctx, FunctionBuilder *function) const {
  if (IsScanPipeline(ctx->GetPipeline())) {
    ScanSorter(ctx, function);
  } else {
    NOISEPAGE_ASSERT(IsBuildPipeline(ctx->GetPipeline()), "Pipeline is unknown to sort translator");
    InsertIntoSorter(ctx, function);
    CounterAdd(function, num_sort_build_rows_, 1);
  }
}

void SortTranslator::FinishPipelineWork(const Pipeline &pipeline, FunctionBuilder *function) const {
  auto *codegen = GetCodeGen();
  ast::Expr *sorter_ptr = global_sorter_.GetPtr(codegen);

  if (IsBuildPipeline(pipeline)) {
    if (build_pipeline_.IsParallel()) {
      if (IsPipelineMetricsEnabled()) {
        auto *exec_ctx = GetExecutionContext();
        auto num_hooks = static_cast<uint32_t>(sql::Sorter::HookOffsets::NUM_HOOKS);
        auto starttlsort = static_cast<uint32_t>(sql::Sorter::HookOffsets::StartTLSortHook);
        auto starttlmerge = static_cast<uint32_t>(sql::Sorter::HookOffsets::StartTLMergeHook);
        auto endtlsort = static_cast<uint32_t>(sql::Sorter::HookOffsets::EndTLSortHook);
        auto endtlmerge = static_cast<uint32_t>(sql::Sorter::HookOffsets::EndTLMergeHook);
        auto endsinglesorter = static_cast<uint32_t>(sql::Sorter::HookOffsets::EndSingleSorterHook);
        function->Append(codegen->ExecCtxInitHooks(exec_ctx, num_hooks));
        function->Append(codegen->ExecCtxRegisterHook(exec_ctx, starttlsort, parallel_starttlsort_hook_fn_));
        function->Append(codegen->ExecCtxRegisterHook(exec_ctx, starttlmerge, parallel_starttlmerge_hook_fn_));
        function->Append(codegen->ExecCtxRegisterHook(exec_ctx, endtlsort, parallel_endtlsort_hook_fn_));
        function->Append(codegen->ExecCtxRegisterHook(exec_ctx, endtlmerge, parallel_endtlmerge_hook_fn_));
        function->Append(codegen->ExecCtxRegisterHook(exec_ctx, endsinglesorter, parallel_endsinglesorter_hook_fn_));
      }

      // Build pipeline is parallel, so we need to issue a parallel sort. Issue
      // a SortParallel() or a SortParallelTopK() depending on whether a limit
      // was provided in the plan.
      ast::Expr *offset = local_sorter_.OffsetFromState(codegen);
      if (const auto &plan = GetPlanAs<planner::OrderByPlanNode>(); plan.HasLimit()) {
        const std::size_t top_k = plan.GetOffset() + plan.GetLimit();
        function->Append(codegen->SortTopKParallel(sorter_ptr, GetThreadStateContainer(), offset, top_k));
      } else {
        function->Append(codegen->SortParallel(sorter_ptr, GetThreadStateContainer(), offset));
      }

      if (IsPipelineMetricsEnabled()) {
        auto *exec_ctx = GetExecutionContext();
        function->Append(codegen->ExecCtxClearHooks(exec_ctx));
      }
    } else {
      function->Append(codegen->SorterSort(sorter_ptr));
      RecordCounters(pipeline, function);
    }
  } else if (!pipeline.IsParallel()) {
    RecordCounters(pipeline, function);
  }

  // TODO(WAN): In theory, we would like to record the true number of unique tuples as the cardinality.
  //  However, due to overhead and engineering complexity, we settle for the size of the sorter.
}

ast::Expr *SortTranslator::GetChildOutput(WorkContext *context, UNUSED_ATTRIBUTE uint32_t child_idx,
                                          uint32_t attr_idx) const {
  if (IsScanPipeline(context->GetPipeline())) {
    return GetSortRowAttribute(sort_row_var_, attr_idx);
  }

  NOISEPAGE_ASSERT(IsBuildPipeline(context->GetPipeline()), "Pipeline not known to sorter");
  switch (current_row_) {
    case CurrentRow::Lhs:
      return GetSortRowAttribute(lhs_row_, attr_idx);
    case CurrentRow::Rhs:
      return GetSortRowAttribute(rhs_row_, attr_idx);
    case CurrentRow::Child: {
      return OperatorTranslator::GetChildOutput(context, child_idx, attr_idx);
    }
  }
  UNREACHABLE("Impossible output row option");
}

}  // namespace noisepage::execution::compiler
