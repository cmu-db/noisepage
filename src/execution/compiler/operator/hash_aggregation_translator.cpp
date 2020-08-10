#include "execution/compiler/operator/hash_aggregation_translator.h"

#include "execution/compiler/codegen.h"
#include "execution/compiler/compilation_context.h"
#include "execution/compiler/function_builder.h"
#include "execution/compiler/if.h"
#include "execution/compiler/loop.h"
#include "execution/compiler/work_context.h"
#include "planner/plannodes/aggregate_plan_node.h"

namespace terrier::execution::compiler {

namespace {
constexpr char GROUP_BY_TERM_ATTR_PREFIX[] = "gb_term_attr";
constexpr char AGGREGATE_TERM_ATTR_PREFIX[] = "agg_term_attr";
}  // namespace

HashAggregationTranslator::HashAggregationTranslator(const planner::AggregatePlanNode &plan,
                                                     CompilationContext *compilation_context, Pipeline *pipeline)
    : OperatorTranslator(plan, compilation_context, pipeline, brain::ExecutionOperatingUnitType::HASH_AGGREGATE),
      agg_row_var_(GetCodeGen()->MakeFreshIdentifier("aggRow")),
      agg_payload_type_(GetCodeGen()->MakeFreshIdentifier("AggPayload")),
      agg_values_type_(GetCodeGen()->MakeFreshIdentifier("AggValues")),
      key_check_fn_(GetCodeGen()->MakeFreshIdentifier(pipeline->CreatePipelineFunctionName("KeyCheck"))),
      key_check_partial_fn_(GetCodeGen()->MakeFreshIdentifier(pipeline->CreatePipelineFunctionName("KeyCheckPartial"))),
      merge_partitions_fn_(GetCodeGen()->MakeFreshIdentifier(pipeline->CreatePipelineFunctionName("MergePartitions"))),
      build_pipeline_(this, Pipeline::Parallelism::Parallel) {
  TERRIER_ASSERT(!plan.GetGroupByTerms().empty(), "Hash aggregation should have grouping keys");
  TERRIER_ASSERT(plan.GetAggregateStrategyType() == planner::AggregateStrategyType::HASH,
                 "Expected hash-based aggregation plan node");
  TERRIER_ASSERT(plan.GetChildrenSize() == 1, "Hash aggregations should only have one child");
  // The produce pipeline begins after the build.
  pipeline->LinkSourcePipeline(&build_pipeline_);

  // Prepare the child.
  compilation_context->Prepare(*plan.GetChild(0), &build_pipeline_);

  // If the build-side is parallel, the produce side is parallel.
  pipeline->RegisterSource(
      this, build_pipeline_.IsParallel() ? Pipeline::Parallelism::Parallel : Pipeline::Parallelism::Serial);

  // Prepare all grouping and aggregate expressions.
  for (const auto group_by_term : plan.GetGroupByTerms()) {
    compilation_context->Prepare(*group_by_term);
  }
  for (const auto agg_term : plan.GetAggregateTerms()) {
    compilation_context->Prepare(*agg_term->GetChild(0));
  }

  // If there's a having clause, prepare it, too.
  if (const auto having_clause = plan.GetHavingClausePredicate(); having_clause != nullptr) {
    compilation_context->Prepare(*having_clause);
  }

  // Declare the global hash table.
  auto *codegen = GetCodeGen();
  ast::Expr *agg_ht_type = codegen->BuiltinType(ast::BuiltinType::AggregationHashTable);
  global_agg_ht_ = compilation_context->GetQueryState()->DeclareStateEntry(codegen, "aggHashTable", agg_ht_type);

  // In parallel mode, declare a local hash table, too.
  if (build_pipeline_.IsParallel()) {
    local_agg_ht_ = build_pipeline_.DeclarePipelineStateEntry("aggHashTable", agg_ht_type);
  }
}

ast::StructDecl *HashAggregationTranslator::GeneratePayloadStruct() {
  auto *codegen = GetCodeGen();
  auto fields = codegen->MakeEmptyFieldList();
  fields.reserve(GetAggPlan().GetGroupByTerms().size() + GetAggPlan().GetAggregateTerms().size());

  // Create a field for every group by term.
  uint32_t term_idx = 0;
  for (const auto &term : GetAggPlan().GetGroupByTerms()) {
    auto field_name = codegen->MakeIdentifier(GROUP_BY_TERM_ATTR_PREFIX + std::to_string(term_idx));
    auto type = codegen->TplType(sql::GetTypeId(term->GetReturnValueType()));
    fields.push_back(codegen->MakeField(field_name, type));
    term_idx++;
  }

  // Create a field for every aggregate term.
  term_idx = 0;
  for (const auto &term : GetAggPlan().GetAggregateTerms()) {
    auto field_name = codegen->MakeIdentifier(AGGREGATE_TERM_ATTR_PREFIX + std::to_string(term_idx));
    auto type = codegen->AggregateType(term->GetExpressionType(), sql::GetTypeId(term->GetReturnValueType()));
    fields.push_back(codegen->MakeField(field_name, type));
    term_idx++;
  }

  return codegen->DeclareStruct(agg_payload_type_, std::move(fields));
}

ast::StructDecl *HashAggregationTranslator::GenerateInputValuesStruct() {
  auto *codegen = GetCodeGen();
  auto fields = codegen->MakeEmptyFieldList();
  fields.reserve(GetAggPlan().GetGroupByTerms().size() + GetAggPlan().GetAggregateTerms().size());

  // Create a field for every group by term.
  uint32_t term_idx = 0;
  for (const auto &term : GetAggPlan().GetGroupByTerms()) {
    auto field_name = codegen->MakeIdentifier(GROUP_BY_TERM_ATTR_PREFIX + std::to_string(term_idx));
    auto type = codegen->TplType(sql::GetTypeId(term->GetReturnValueType()));
    fields.push_back(codegen->MakeField(field_name, type));
    term_idx++;
  }

  // Create a field for every aggregate term.
  term_idx = 0;
  for (const auto &term : GetAggPlan().GetAggregateTerms()) {
    auto field_name = codegen->MakeIdentifier(AGGREGATE_TERM_ATTR_PREFIX + std::to_string(term_idx));
    auto type = codegen->TplType(sql::GetTypeId(term->GetChild(0)->GetReturnValueType()));
    fields.push_back(codegen->MakeField(field_name, type));
    term_idx++;
  }

  return codegen->DeclareStruct(agg_values_type_, std::move(fields));
}

void HashAggregationTranslator::DefineHelperStructs(util::RegionVector<ast::StructDecl *> *decls) {
  decls->push_back(GeneratePayloadStruct());
  decls->push_back(GenerateInputValuesStruct());
}

void HashAggregationTranslator::MergeOverflowPartitions(FunctionBuilder *function, ast::Expr *agg_ht, ast::Expr *iter) {
  auto *codegen = GetCodeGen();

  Loop loop(function, nullptr, codegen->AggPartitionIteratorHasNext(iter),
            codegen->MakeStmt(codegen->AggPartitionIteratorNext(iter)));
  {
    // Get hash from overflow entry.
    auto hash_val = codegen->MakeFreshIdentifier("hashVal");
    function->Append(codegen->DeclareVarWithInit(hash_val, codegen->AggPartitionIteratorGetHash(iter)));

    // Get the partial aggregate row from the overflow entry.
    auto partial_row = codegen->MakeFreshIdentifier("partialRow");
    function->Append(
        codegen->DeclareVarWithInit(partial_row, codegen->AggPartitionIteratorGetRow(iter, agg_payload_type_)));

    // Perform lookup.
    auto lookup_result = codegen->MakeFreshIdentifier("aggPayload");
    function->Append(codegen->DeclareVarWithInit(
        lookup_result, codegen->AggHashTableLookup(agg_ht, codegen->MakeExpr(hash_val), key_check_partial_fn_,
                                                   codegen->MakeExpr(partial_row), agg_payload_type_)));

    If check_found(function, codegen->IsNilPointer(codegen->MakeExpr(lookup_result)));
    {
      // Link entry.
      auto entry = codegen->AggPartitionIteratorGetRowEntry(iter);
      function->Append(codegen->AggHashTableLinkEntry(agg_ht, entry));
    }
    check_found.Else();
    {
      // Merge partial aggregate.
      for (uint32_t term_idx = 0; term_idx < GetAggPlan().GetAggregateTerms().size(); term_idx++) {
        auto lhs = GetAggregateTermPtr(lookup_result, term_idx);
        auto rhs = GetAggregateTermPtr(partial_row, term_idx);
        function->Append(codegen->AggregatorMerge(lhs, rhs));
      }
    }
    check_found.EndIf();
  }
}

ast::FunctionDecl *HashAggregationTranslator::GeneratePartialKeyCheckFunction() {
  auto *codegen = GetCodeGen();

  auto lhs_arg = codegen->MakeIdentifier("lhs");
  auto rhs_arg = codegen->MakeIdentifier("rhs");
  auto params = codegen->MakeFieldList({
      codegen->MakeField(lhs_arg, codegen->PointerType(agg_payload_type_)),
      codegen->MakeField(rhs_arg, codegen->PointerType(agg_payload_type_)),
  });
  auto ret_type = codegen->BuiltinType(ast::BuiltinType::Kind::Bool);
  FunctionBuilder builder(codegen, key_check_partial_fn_, std::move(params), ret_type);
  {
    for (uint32_t term_idx = 0; term_idx < GetAggPlan().GetGroupByTerms().size(); term_idx++) {
      auto lhs = GetGroupByTerm(lhs_arg, term_idx);
      auto rhs = GetGroupByTerm(rhs_arg, term_idx);
      If check_match(&builder, codegen->Compare(parsing::Token::Type::BANG_EQUAL, lhs, rhs));
      builder.Append(codegen->Return(codegen->ConstBool(false)));
    }
    builder.Append(codegen->Return(codegen->ConstBool(true)));
  }
  return builder.Finish();
}

ast::FunctionDecl *HashAggregationTranslator::GenerateMergeOverflowPartitionsFunction() {
  // The partition merge function has the following signature:
  // (*QueryState, *AggregationHashTable, *AHTOverflowPartitionIterator) -> nil

  auto *codegen = GetCodeGen();
  auto params = GetCompilationContext()->QueryParams();

  // Then the aggregation hash table and the overflow partition iterator.
  auto agg_ht = codegen->MakeIdentifier("aggHashTable");
  auto overflow_iter = codegen->MakeIdentifier("ahtOvfIter");
  params.push_back(codegen->MakeField(agg_ht, codegen->PointerType(ast::BuiltinType::AggregationHashTable)));
  params.push_back(
      codegen->MakeField(overflow_iter, codegen->PointerType(ast::BuiltinType::AHTOverflowPartitionIterator)));

  auto ret_type = codegen->BuiltinType(ast::BuiltinType::Kind::Nil);
  FunctionBuilder builder(codegen, merge_partitions_fn_, std::move(params), ret_type);
  {
    // Main merging logic.
    MergeOverflowPartitions(&builder, codegen->MakeExpr(agg_ht), codegen->MakeExpr(overflow_iter));
  }
  return builder.Finish();
}

ast::FunctionDecl *HashAggregationTranslator::GenerateKeyCheckFunction() {
  auto *codegen = GetCodeGen();
  auto agg_payload = codegen->MakeIdentifier("aggPayload");
  auto agg_values = codegen->MakeIdentifier("aggValues");
  auto params = codegen->MakeFieldList({
      codegen->MakeField(agg_payload, codegen->PointerType(agg_payload_type_)),
      codegen->MakeField(agg_values, codegen->PointerType(agg_values_type_)),
  });
  auto ret_type = codegen->BuiltinType(ast::BuiltinType::Kind::Bool);
  FunctionBuilder builder(codegen, key_check_fn_, std::move(params), ret_type);
  {
    for (uint32_t term_idx = 0; term_idx < GetAggPlan().GetGroupByTerms().size(); term_idx++) {
      auto lhs = GetGroupByTerm(agg_payload, term_idx);
      auto rhs = GetGroupByTerm(agg_values, term_idx);
      If check_match(&builder, codegen->Compare(parsing::Token::Type::BANG_EQUAL, lhs, rhs));
      builder.Append(codegen->Return(codegen->ConstBool(false)));
    }
    builder.Append(codegen->Return(codegen->ConstBool(true)));
  }
  return builder.Finish();
}

void HashAggregationTranslator::DefineHelperFunctions(util::RegionVector<ast::FunctionDecl *> *decls) {
  if (build_pipeline_.IsParallel()) {
    decls->push_back(GeneratePartialKeyCheckFunction());
    decls->push_back(GenerateMergeOverflowPartitionsFunction());
  }
  decls->push_back(GenerateKeyCheckFunction());
}

void HashAggregationTranslator::InitializeAggregationHashTable(FunctionBuilder *function, ast::Expr *agg_ht) const {
  function->Append(GetCodeGen()->AggHashTableInit(agg_ht, GetExecutionContext(), GetMemoryPool(), agg_payload_type_));
}

void HashAggregationTranslator::TearDownAggregationHashTable(FunctionBuilder *function, ast::Expr *agg_ht) const {
  function->Append(GetCodeGen()->AggHashTableFree(agg_ht));
}

void HashAggregationTranslator::InitializeQueryState(FunctionBuilder *function) const {
  InitializeAggregationHashTable(function, global_agg_ht_.GetPtr(GetCodeGen()));
}

void HashAggregationTranslator::TearDownQueryState(FunctionBuilder *function) const {
  TearDownAggregationHashTable(function, global_agg_ht_.GetPtr(GetCodeGen()));
}

void HashAggregationTranslator::InitializePipelineState(const Pipeline &pipeline, FunctionBuilder *function) const {
  if (IsBuildPipeline(pipeline) && build_pipeline_.IsParallel()) {
    InitializeAggregationHashTable(function, local_agg_ht_.GetPtr(GetCodeGen()));
  }
}

void HashAggregationTranslator::TearDownPipelineState(const Pipeline &pipeline, FunctionBuilder *function) const {
  if (IsBuildPipeline(pipeline) && build_pipeline_.IsParallel()) {
    TearDownAggregationHashTable(function, local_agg_ht_.GetPtr(GetCodeGen()));
  }
}

ast::Expr *HashAggregationTranslator::GetGroupByTerm(ast::Identifier agg_row, uint32_t attr_idx) const {
  auto *codegen = GetCodeGen();
  auto member = codegen->MakeIdentifier(GROUP_BY_TERM_ATTR_PREFIX + std::to_string(attr_idx));
  return codegen->AccessStructMember(codegen->MakeExpr(agg_row), member);
}

ast::Expr *HashAggregationTranslator::GetAggregateTerm(ast::Identifier agg_row, uint32_t attr_idx) const {
  auto *codegen = GetCodeGen();
  auto member = codegen->MakeIdentifier(AGGREGATE_TERM_ATTR_PREFIX + std::to_string(attr_idx));
  return codegen->AccessStructMember(codegen->MakeExpr(agg_row), member);
}

ast::Expr *HashAggregationTranslator::GetAggregateTermPtr(ast::Identifier agg_row, uint32_t attr_idx) const {
  return GetCodeGen()->AddressOf(GetAggregateTerm(agg_row, attr_idx));
}

ast::Identifier HashAggregationTranslator::FillInputValues(FunctionBuilder *function, WorkContext *ctx) const {
  auto *codegen = GetCodeGen();

  // var aggValues : AggValues
  auto agg_values = codegen->MakeFreshIdentifier("aggValues");
  function->Append(codegen->DeclareVarNoInit(agg_values, codegen->MakeExpr(agg_values_type_)));

  // Populate the grouping terms.
  uint32_t term_idx = 0;
  for (const auto &term : GetAggPlan().GetGroupByTerms()) {
    auto lhs = GetGroupByTerm(agg_values, term_idx);
    auto rhs = ctx->DeriveValue(*term, this);
    function->Append(codegen->Assign(lhs, rhs));
    term_idx++;
  }

  // Populate the raw aggregate values.
  term_idx = 0;
  for (const auto &term : GetAggPlan().GetAggregateTerms()) {
    auto lhs = GetAggregateTerm(agg_values, term_idx);
    auto rhs = ctx->DeriveValue(*term->GetChild(0), this);
    function->Append(codegen->Assign(lhs, rhs));
    term_idx++;
  }

  return agg_values;
}

ast::Identifier HashAggregationTranslator::HashInputKeys(FunctionBuilder *function, ast::Identifier agg_values) const {
  std::vector<ast::Expr *> keys;
  for (uint32_t term_idx = 0; term_idx < GetAggPlan().GetGroupByTerms().size(); term_idx++) {
    keys.push_back(GetGroupByTerm(agg_values, term_idx));
  }

  // var hashVal = @hash(...)
  auto *codegen = GetCodeGen();
  auto hash_val = codegen->MakeFreshIdentifier("hashVal");
  function->Append(codegen->DeclareVarWithInit(hash_val, codegen->Hash(keys)));
  return hash_val;
}

ast::Identifier HashAggregationTranslator::PerformLookup(FunctionBuilder *function, ast::Expr *agg_ht,
                                                         ast::Identifier hash_val, ast::Identifier agg_values) const {
  auto *codegen = GetCodeGen();
  // var aggPayload = @ptrCast(*AggPayload, @aggHTLookup())
  auto lookup_call = codegen->AggHashTableLookup(agg_ht, codegen->MakeExpr(hash_val), key_check_fn_,
                                                 codegen->AddressOf(codegen->MakeExpr(agg_values)), agg_payload_type_);
  auto agg_payload = codegen->MakeFreshIdentifier("aggPayload");
  function->Append(codegen->DeclareVarWithInit(agg_payload, lookup_call));
  return agg_payload;
}

void HashAggregationTranslator::ConstructNewAggregate(FunctionBuilder *function, ast::Expr *agg_ht,
                                                      ast::Identifier agg_payload, ast::Identifier agg_values,
                                                      ast::Identifier hash_val) const {
  auto *codegen = GetCodeGen();

  // aggRow = @ptrCast(*AggPayload, @aggHTInsert(&state.agg_table, agg_hash_val))
  bool partitioned = build_pipeline_.IsParallel();
  auto insert_call = codegen->AggHashTableInsert(agg_ht, codegen->MakeExpr(hash_val), partitioned, agg_payload_type_);
  function->Append(codegen->Assign(codegen->MakeExpr(agg_payload), insert_call));

  // Copy the grouping keys.
  for (uint32_t term_idx = 0; term_idx < GetAggPlan().GetGroupByTerms().size(); term_idx++) {
    auto lhs = GetGroupByTerm(agg_payload, term_idx);
    auto rhs = GetGroupByTerm(agg_values, term_idx);
    function->Append(codegen->Assign(lhs, rhs));
  }

  // Initialize all aggregate terms.
  for (uint32_t term_idx = 0; term_idx < GetAggPlan().GetAggregateTerms().size(); term_idx++) {
    auto agg_term = GetAggregateTermPtr(agg_payload, term_idx);
    function->Append(codegen->AggregatorInit(agg_term));
  }
}

void HashAggregationTranslator::AdvanceAggregate(FunctionBuilder *function, ast::Identifier agg_payload,
                                                 ast::Identifier agg_values) const {
  auto *codegen = GetCodeGen();
  for (uint32_t term_idx = 0; term_idx < GetAggPlan().GetAggregateTerms().size(); term_idx++) {
    auto agg = GetAggregateTermPtr(agg_payload, term_idx);
    auto val = GetAggregateTermPtr(agg_values, term_idx);
    function->Append(codegen->AggregatorAdvance(agg, val));
  }
}

void HashAggregationTranslator::UpdateAggregates(WorkContext *context, FunctionBuilder *function,
                                                 ast::Expr *agg_ht) const {
  auto *codegen = GetCodeGen();

  auto agg_values = FillInputValues(function, context);
  auto hash_val = HashInputKeys(function, agg_values);
  auto agg_payload = PerformLookup(function, agg_ht, hash_val, agg_values);

  If check_new_agg(function, codegen->IsNilPointer(codegen->MakeExpr(agg_payload)));
  ConstructNewAggregate(function, agg_ht, agg_payload, agg_values, hash_val);
  check_new_agg.EndIf();

  // Advance aggregate.
  AdvanceAggregate(function, agg_payload, agg_values);
}

void HashAggregationTranslator::ScanAggregationHashTable(WorkContext *context, FunctionBuilder *function,
                                                         ast::Expr *agg_ht) const {
  auto *codegen = GetCodeGen();

  // var iterBase: AHTIterator
  ast::Identifier aht_iter_base = codegen->MakeFreshIdentifier("iterBase");
  ast::Expr *aht_iter_type = codegen->BuiltinType(ast::BuiltinType::AHTIterator);
  function->Append(codegen->DeclareVarNoInit(aht_iter_base, aht_iter_type));

  // var ahtIter = &ahtIterBase
  ast::Identifier aht_iter = codegen->MakeFreshIdentifier("iter");
  ast::Expr *aht_iter_init = codegen->AddressOf(codegen->MakeExpr(aht_iter_base));
  function->Append(codegen->DeclareVarWithInit(aht_iter, aht_iter_init));

  Loop loop(function, codegen->MakeStmt(codegen->AggHashTableIteratorInit(codegen->MakeExpr(aht_iter), agg_ht)),
            codegen->AggHashTableIteratorHasNext(codegen->MakeExpr(aht_iter)),
            codegen->MakeStmt(codegen->AggHashTableIteratorNext(codegen->MakeExpr(aht_iter))));
  {
    // var aggRow = @ahtIterGetRow()
    function->Append(codegen->DeclareVarWithInit(
        agg_row_var_, codegen->AggHashTableIteratorGetRow(codegen->MakeExpr(aht_iter), agg_payload_type_)));

    // Check having clause.
    if (const auto having = GetAggPlan().GetHavingClausePredicate(); having != nullptr) {
      If check_having(function, context->DeriveValue(*having, this));
      context->Push(function);
    } else {
      context->Push(function);
    }
  }
  loop.EndLoop();

  // Close iterator.
  function->Append(codegen->AggHashTableIteratorClose(codegen->MakeExpr(aht_iter)));
}

void HashAggregationTranslator::PerformPipelineWork(WorkContext *context, FunctionBuilder *function) const {
  auto *codegen = GetCodeGen();
  if (IsBuildPipeline(context->GetPipeline())) {
    const auto &agg_ht = build_pipeline_.IsParallel() ? local_agg_ht_ : global_agg_ht_;
    UpdateAggregates(context, function, agg_ht.GetPtr(codegen));
  } else {
    TERRIER_ASSERT(IsProducePipeline(context->GetPipeline()), "Pipeline is unknown to hash aggregation translator");
    if (GetPipeline()->IsParallel()) {
      // In parallel-mode, we would've issued a parallel partitioned scan. In
      // this case, the aggregation hash table we're to scan is provided as a
      // function parameter; specifically, the last argument in the worker
      // function which we're generating right now. Pull it out.
      auto agg_ht_param_position = GetPipeline()->PipelineParams().size();
      auto agg_ht = function->GetParameterByPosition(agg_ht_param_position);
      ScanAggregationHashTable(context, function, agg_ht);
    } else {
      ScanAggregationHashTable(context, function, global_agg_ht_.GetPtr(codegen));
    }
  }
}

void HashAggregationTranslator::FinishPipelineWork(const Pipeline &pipeline, FunctionBuilder *function) const {
  if (IsBuildPipeline(pipeline) && build_pipeline_.IsParallel()) {
    auto *codegen = GetCodeGen();
    auto global_agg_ht = global_agg_ht_.GetPtr(codegen);
    auto thread_state_container = GetThreadStateContainer();
    auto tl_agg_ht_offset = local_agg_ht_.OffsetFromState(codegen);
    function->Append(codegen->AggHashTableMovePartitions(global_agg_ht, thread_state_container, tl_agg_ht_offset,
                                                         merge_partitions_fn_));
  }
}

ast::Expr *HashAggregationTranslator::GetChildOutput(WorkContext *context, uint32_t child_idx,
                                                     uint32_t attr_idx) const {
  if (IsProducePipeline(context->GetPipeline())) {
    if (child_idx == 0) {
      return GetGroupByTerm(agg_row_var_, attr_idx);
    }
    return GetCodeGen()->AggregatorResult(GetAggregateTermPtr(agg_row_var_, attr_idx));
  }
  // The request is in the build pipeline. Forward to child translator.
  return OperatorTranslator::GetChildOutput(context, child_idx, attr_idx);
}

util::RegionVector<ast::FieldDecl *> HashAggregationTranslator::GetWorkerParams() const {
  TERRIER_ASSERT(build_pipeline_.IsParallel(), "Should not issue parallel scan if pipeline isn't parallelized.");
  auto *codegen = GetCodeGen();
  return codegen->MakeFieldList({codegen->MakeField(codegen->MakeIdentifier("aggHashTable"),
                                                    codegen->PointerType(ast::BuiltinType::AggregationHashTable))});
}

void HashAggregationTranslator::LaunchWork(FunctionBuilder *function, ast::Identifier work_func_name) const {
  TERRIER_ASSERT(build_pipeline_.IsParallel(), "Should not issue parallel scan if pipeline isn't parallelized.");
  auto *codegen = GetCodeGen();
  function->Append(codegen->AggHashTableParallelScan(global_agg_ht_.GetPtr(codegen), GetQueryStatePtr(),
                                                     GetThreadStateContainer(), work_func_name));
}

}  // namespace terrier::execution::compiler
