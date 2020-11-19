#include "execution/compiler/operator/hash_join_translator.h"

#include "execution/ast/type.h"
#include "execution/compiler/compilation_context.h"
#include "execution/compiler/function_builder.h"
#include "execution/compiler/if.h"
#include "execution/compiler/loop.h"
#include "execution/compiler/work_context.h"
#include "execution/sql/join_hash_table.h"
#include "planner/plannodes/hash_join_plan_node.h"
#include "planner/plannodes/output_schema.h"

namespace noisepage::execution::compiler {

namespace {
const char *row_attr_prefix = "attr";
}  // namespace

HashJoinTranslator::HashJoinTranslator(const planner::HashJoinPlanNode &plan, CompilationContext *compilation_context,
                                       Pipeline *pipeline)
    // The ExecutionOperatingUnitType depends on whether it is the build pipeline or probe pipeline.
    : OperatorTranslator(plan, compilation_context, pipeline, selfdriving::ExecutionOperatingUnitType::DUMMY),
      join_consumer_flag_(false),
      build_row_var_(GetCodeGen()->MakeFreshIdentifier("buildRow")),
      build_row_type_(GetCodeGen()->MakeFreshIdentifier("BuildRow")),
      build_mark_(GetCodeGen()->MakeFreshIdentifier("buildMark")),
      probe_row_var_(GetCodeGen()->MakeFreshIdentifier("probeRow")),
      probe_row_type_(GetCodeGen()->MakeFreshIdentifier("ProbeRow")),
      join_consumer_(GetCodeGen()->MakeFreshIdentifier("joinConsumer")),
      left_pipeline_(this, Pipeline::Parallelism::Parallel) {
  NOISEPAGE_ASSERT(!plan.GetLeftHashKeys().empty(), "Hash-join must have join keys from left input");
  NOISEPAGE_ASSERT(!plan.GetRightHashKeys().empty(), "Hash-join must have join keys from right input");
  NOISEPAGE_ASSERT(plan.GetJoinPredicate() != nullptr, "Hash-join must have a join predicate!");

  // Probe pipeline begins after build pipeline.
  pipeline->LinkSourcePipeline(&left_pipeline_);
  // Register left and right child in their appropriate pipelines.
  compilation_context->Prepare(*plan.GetChild(0), &left_pipeline_);
  compilation_context->Prepare(*plan.GetChild(1), pipeline);

  // Prepare join predicate, left, and right hash keys.
  compilation_context->Prepare(*plan.GetJoinPredicate());
  for (const auto left_hash_key : plan.GetLeftHashKeys()) {
    compilation_context->Prepare(*left_hash_key);
  }
  for (const auto right_hash_key : plan.GetRightHashKeys()) {
    compilation_context->Prepare(*right_hash_key);
  }

  // Declare global state.
  auto *codegen = GetCodeGen();
  ast::Expr *join_ht_type = codegen->BuiltinType(ast::BuiltinType::JoinHashTable);
  global_join_ht_ = compilation_context->GetQueryState()->DeclareStateEntry(codegen, "joinHashTable", join_ht_type);

  if (left_pipeline_.IsParallel()) {
    local_join_ht_ = left_pipeline_.DeclarePipelineStateEntry("joinHashTable", join_ht_type);
  }

  num_build_rows_ = CounterDeclare("num_build_rows", &left_pipeline_);
  num_probe_rows_ = CounterDeclare("num_probe_rows", pipeline);
  num_match_rows_ = CounterDeclare("num_match_rows", pipeline);

  if (left_pipeline_.IsParallel() && IsPipelineMetricsEnabled()) {
    parallel_build_pre_hook_fn_ =
        GetCodeGen()->MakeFreshIdentifier(left_pipeline_.CreatePipelineFunctionName("PreHook"));
    parallel_build_post_hook_fn_ =
        GetCodeGen()->MakeFreshIdentifier(left_pipeline_.CreatePipelineFunctionName("PostHook"));
  }
}

void HashJoinTranslator::DefineHelperStructs(util::RegionVector<ast::StructDecl *> *decls) {
  auto *codegen = GetCodeGen();

  /* Build row declaration */
  auto fields = codegen->MakeEmptyFieldList();
  GetAllChildOutputFields(0, row_attr_prefix, &fields);
  if (GetPlanAs<planner::HashJoinPlanNode>().RequiresLeftMark()) {
    fields.push_back(codegen->MakeField(build_mark_, codegen->BoolType()));
  }
  ast::StructDecl *struct_decl = codegen->DeclareStruct(build_row_type_, std::move(fields));
  struct_decl_ = struct_decl;
  decls->push_back(struct_decl);

  /* Probe row declaration - only for left outer joins */
  if (GetPlanAs<planner::HashJoinPlanNode>().GetLogicalJoinType() == planner::LogicalJoinType::LEFT) {
    // TODO(abalakum): support mini-runners for this struct as well
    fields = codegen->MakeEmptyFieldList();
    GetAllChildOutputFields(1, row_attr_prefix, &fields);
    struct_decl = codegen->DeclareStruct(probe_row_type_, std::move(fields));
    decls->push_back(struct_decl);
  }
}

void HashJoinTranslator::DefineHelperFunctions(util::RegionVector<ast::FunctionDecl *> *decls) {
  if (GetPlanAs<planner::HashJoinPlanNode>().GetLogicalJoinType() == planner::LogicalJoinType::LEFT) {
    auto cc = GetCompilationContext();
    auto *pipeline = GetPipeline();
    // Create a WorkContext and make the state identical to the WorkContext generated inside
    // of PerformPipelineWork
    WorkContext ctx(cc, *pipeline);
    ctx.SetSource(this);
    auto *codegen = GetCodeGen();
    util::RegionVector<ast::FieldDecl *> params = pipeline->PipelineParams();
    params.push_back(codegen->MakeField(build_row_var_, codegen->PointerType(build_row_type_)));
    params.push_back(codegen->MakeField(probe_row_var_, codegen->PointerType(probe_row_type_)));
    join_consumer_flag_ = true;
    FunctionBuilder function(codegen, join_consumer_, std::move(params), codegen->Nil());
    {
      // Push to parent
      ctx.Push(&function);
    }
    join_consumer_flag_ = false;
    decls->push_back(function.Finish());
  }
}

ast::FunctionDecl *HashJoinTranslator::GenerateStartHookFunction() const {
  auto *codegen = GetCodeGen();
  auto *pipeline = &left_pipeline_;

  auto params = GetHookParams(left_pipeline_, nullptr, nullptr);
  auto ret_type = codegen->BuiltinType(ast::BuiltinType::Kind::Nil);
  FunctionBuilder builder(codegen, parallel_build_pre_hook_fn_, std::move(params), ret_type);
  { pipeline->InjectStartResourceTracker(&builder, true); }
  return builder.Finish();
}

ast::FunctionDecl *HashJoinTranslator::GenerateEndHookFunction() const {
  auto *codegen = GetCodeGen();
  auto *pipeline = &left_pipeline_;

  auto override_value = codegen->MakeIdentifier("overrideValue");
  auto uint32_type = codegen->BuiltinType(ast::BuiltinType::Uint32);
  auto params = GetHookParams(left_pipeline_, &override_value, uint32_type);

  auto ret_type = codegen->BuiltinType(ast::BuiltinType::Kind::Nil);
  FunctionBuilder builder(codegen, parallel_build_post_hook_fn_, std::move(params), ret_type);
  {
    // FeatureRecord with the overrideValue
    FeatureRecord(&builder, selfdriving::ExecutionOperatingUnitType::PARALLEL_MERGE_HASHJOIN,
                  selfdriving::ExecutionOperatingUnitFeatureAttribute::NUM_ROWS, *pipeline,
                  codegen->MakeExpr(override_value));
    FeatureRecord(&builder, selfdriving::ExecutionOperatingUnitType::PARALLEL_MERGE_HASHJOIN,
                  selfdriving::ExecutionOperatingUnitFeatureAttribute::CARDINALITY, *pipeline,
                  codegen->MakeExpr(override_value));

    // End Tracker
    pipeline->InjectEndResourceTracker(&builder, true);
  }
  return builder.Finish();
}

void HashJoinTranslator::DefineTLSDependentHelperFunctions(const Pipeline &pipeline,
                                                           util::RegionVector<ast::FunctionDecl *> *decls) {
  if (IsLeftPipeline(pipeline) && left_pipeline_.IsParallel() && IsPipelineMetricsEnabled()) {
    decls->push_back(GenerateStartHookFunction());
    decls->push_back(GenerateEndHookFunction());
  }
}

void HashJoinTranslator::InitializeJoinHashTable(FunctionBuilder *function, ast::Expr *jht_ptr) const {
  function->Append(GetCodeGen()->JoinHashTableInit(jht_ptr, GetExecutionContext(), build_row_type_));
}

void HashJoinTranslator::TearDownJoinHashTable(FunctionBuilder *function, ast::Expr *jht_ptr) const {
  function->Append(GetCodeGen()->JoinHashTableFree(jht_ptr));
}

void HashJoinTranslator::InitializeQueryState(FunctionBuilder *function) const {
  auto *codegen = GetCodeGen();
  InitializeJoinHashTable(function, global_join_ht_.GetPtr(codegen));
}

void HashJoinTranslator::TearDownQueryState(FunctionBuilder *function) const {
  TearDownJoinHashTable(function, global_join_ht_.GetPtr(GetCodeGen()));
}

void HashJoinTranslator::InitializePipelineState(const Pipeline &pipeline, FunctionBuilder *function) const {
  if (IsLeftPipeline(pipeline) && left_pipeline_.IsParallel()) {
    InitializeJoinHashTable(function, local_join_ht_.GetPtr(GetCodeGen()));
  }

  InitializeCounters(pipeline, function);
}

void HashJoinTranslator::TearDownPipelineState(const Pipeline &pipeline, FunctionBuilder *function) const {
  if (IsLeftPipeline(pipeline) && left_pipeline_.IsParallel()) {
    TearDownJoinHashTable(function, local_join_ht_.GetPtr(GetCodeGen()));
  }
}

void HashJoinTranslator::InitializeCounters(const Pipeline &pipeline, FunctionBuilder *function) const {
  if (IsLeftPipeline(pipeline)) {
    CounterSet(function, num_build_rows_, 0);
  } else {
    CounterSet(function, num_probe_rows_, 0);
    CounterSet(function, num_match_rows_, 0);
  }
}

void HashJoinTranslator::RecordCounters(const Pipeline &pipeline, FunctionBuilder *function) const {
  if (IsLeftPipeline(pipeline)) {
    FeatureRecord(function, selfdriving::ExecutionOperatingUnitType::HASHJOIN_BUILD,
                  selfdriving::ExecutionOperatingUnitFeatureAttribute::NUM_ROWS, pipeline, CounterVal(num_build_rows_));
    FeatureRecord(function, selfdriving::ExecutionOperatingUnitType::HASHJOIN_BUILD,
                  selfdriving::ExecutionOperatingUnitFeatureAttribute::CARDINALITY, pipeline,
                  CounterVal(num_build_rows_));
    FeatureArithmeticRecordMul(function, pipeline, GetTranslatorId(), CounterVal(num_build_rows_));
  } else {
    FeatureRecord(function, selfdriving::ExecutionOperatingUnitType::HASHJOIN_PROBE,
                  selfdriving::ExecutionOperatingUnitFeatureAttribute::NUM_ROWS, pipeline, CounterVal(num_probe_rows_));
    FeatureRecord(function, selfdriving::ExecutionOperatingUnitType::HASHJOIN_PROBE,
                  selfdriving::ExecutionOperatingUnitFeatureAttribute::CARDINALITY, pipeline,
                  CounterVal(num_match_rows_));
    FeatureArithmeticRecordSet(function, pipeline, GetTranslatorId(), CounterVal(num_match_rows_));
  }
}

void HashJoinTranslator::EndParallelPipelineWork(const Pipeline &pipeline, FunctionBuilder *function) const {
  RecordCounters(pipeline, function);
}

ast::Expr *HashJoinTranslator::HashKeys(
    WorkContext *ctx, FunctionBuilder *function,
    const std::vector<common::ManagedPointer<parser::AbstractExpression>> &hash_keys) const {
  auto *codegen = GetCodeGen();

  std::vector<ast::Expr *> key_values;
  key_values.reserve(hash_keys.size());
  for (const auto hash_key : hash_keys) {
    key_values.push_back(ctx->DeriveValue(*hash_key, this));
  }

  ast::Identifier hash_val_name = codegen->MakeFreshIdentifier("hashVal");
  function->Append(codegen->DeclareVarWithInit(hash_val_name, codegen->Hash(key_values)));

  return codegen->MakeExpr(hash_val_name);
}

ast::Expr *HashJoinTranslator::GetRowAttribute(ast::Expr *row, uint32_t attr_idx) const {
  auto *codegen = GetCodeGen();
  auto attr_name = codegen->MakeIdentifier(row_attr_prefix + std::to_string(attr_idx));
  return codegen->AccessStructMember(row, attr_name);
}

void HashJoinTranslator::FillBuildRow(WorkContext *ctx, FunctionBuilder *function, ast::Expr *build_row) const {
  auto *codegen = GetCodeGen();
  const auto child_schema = GetPlan().GetChild(0)->GetOutputSchema();
  for (uint32_t attr_idx = 0; attr_idx < child_schema->GetColumns().size(); attr_idx++) {
    ast::Expr *lhs = GetRowAttribute(build_row, attr_idx);
    ast::Expr *rhs = GetChildOutput(ctx, 0, attr_idx);
    function->Append(codegen->Assign(lhs, rhs));
  }
  const auto &join_plan = GetPlanAs<planner::HashJoinPlanNode>();
  if (join_plan.RequiresLeftMark()) {
    ast::Expr *lhs = codegen->AccessStructMember(build_row, build_mark_);
    ast::Expr *rhs = codegen->ConstBool(true);
    function->Append(codegen->Assign(lhs, rhs));
  }
}

void HashJoinTranslator::FillProbeRow(WorkContext *ctx, FunctionBuilder *function, ast::Expr *probe_row) const {
  auto *codegen = GetCodeGen();
  const auto child_schema = GetPlan().GetChild(1)->GetOutputSchema();
  for (uint32_t attr_idx = 0; attr_idx < child_schema->GetColumns().size(); attr_idx++) {
    ast::Expr *lhs = GetRowAttribute(probe_row, attr_idx);
    ast::Expr *rhs = GetChildOutput(ctx, 1, attr_idx);
    function->Append(codegen->Assign(lhs, rhs));
  }
}

void HashJoinTranslator::InsertIntoJoinHashTable(WorkContext *ctx, FunctionBuilder *function) const {
  auto *codegen = GetCodeGen();

  const auto join_ht = ctx->GetPipeline().IsParallel() ? local_join_ht_ : global_join_ht_;

  // var hashVal = @hash(...)
  auto hash_val = HashKeys(ctx, function, GetPlanAs<planner::HashJoinPlanNode>().GetLeftHashKeys());

  // var buildRow = @joinHTInsert(...)
  function->Append(codegen->DeclareVarWithInit(
      build_row_var_, codegen->JoinHashTableInsert(join_ht.GetPtr(codegen), hash_val, build_row_type_)));

  // Fill row.
  FillBuildRow(ctx, function, codegen->MakeExpr(build_row_var_));

  CounterAdd(function, num_build_rows_, 1);
}

void HashJoinTranslator::ProbeJoinHashTable(WorkContext *ctx, FunctionBuilder *function) const {
  auto *codegen = GetCodeGen();

  // var entryIterBase: HashTableEntryIterator
  auto iter_name_base = codegen->MakeFreshIdentifier("entryIterBase");
  function->Append(codegen->DeclareVarNoInit(iter_name_base, ast::BuiltinType::HashTableEntryIterator));

  // var entryIter = &entryIterBase
  auto iter_name = codegen->MakeFreshIdentifier("entryIter");
  function->Append(codegen->DeclareVarWithInit(iter_name, codegen->AddressOf(codegen->MakeExpr(iter_name_base))));

  auto entry_iter = codegen->MakeExpr(iter_name);
  auto hash_val = HashKeys(ctx, function, GetPlanAs<planner::HashJoinPlanNode>().GetRightHashKeys());

  // Probe matches.
  const auto &join_plan = GetPlanAs<planner::HashJoinPlanNode>();
  auto lookup_call =
      codegen->MakeStmt(codegen->JoinHashTableLookup(global_join_ht_.GetPtr(codegen), entry_iter, hash_val));
  auto has_next_call = codegen->HTEntryIterHasNext(entry_iter);

  CounterAdd(function, num_probe_rows_, 1);

  // The probe depends on the join type
  if (join_plan.RequiresRightMark()) {
    // First declare the right mark.
    ast::Identifier right_mark_var = codegen->MakeFreshIdentifier("rightMark");
    ast::Expr *right_mark = codegen->MakeExpr(right_mark_var);
    function->Append(codegen->DeclareVarWithInit(right_mark_var, codegen->ConstBool(true)));

    // Probe hash table and check for a match. Loop condition becomes false as
    // soon as a match is found.
    auto loop_cond = codegen->BinaryOp(parsing::Token::Type::AND, right_mark, has_next_call);
    Loop entry_loop(function, lookup_call, loop_cond, nullptr);
    {
      // var buildRow = @ptrCast(*BuildRow, @htEntryIterGetRow())
      function->Append(
          codegen->DeclareVarWithInit(build_row_var_, codegen->HTEntryIterGetRow(entry_iter, build_row_type_)));
      CheckRightMark(ctx, function, right_mark_var);
    }
    entry_loop.EndLoop();

    // The next step depends on the join type.
    if (join_plan.GetLogicalJoinType() == planner::LogicalJoinType::RIGHT_ANTI) {
      // If the right mark is true, then we can perform the anti join.
      // if (right_mark)
      If right_anti_check(function, codegen->MakeExpr(right_mark_var));
      ctx->Push(function);
      CounterAdd(function, num_match_rows_, 1);
      right_anti_check.EndIf();
    } else if (join_plan.GetLogicalJoinType() == planner::LogicalJoinType::RIGHT_SEMI) {
      // If the right mark is unset, then there is at least one match.
      // if (!right_mark)
      auto cond = codegen->UnaryOp(parsing::Token::Type::BANG, codegen->MakeExpr(right_mark_var));
      If right_semi_check(function, cond);
      ctx->Push(function);
      CounterAdd(function, num_match_rows_, 1);
      right_semi_check.EndIf();
    }
  } else {
    // For regular joins: while (has_next)
    Loop entry_loop(function, lookup_call, has_next_call, nullptr);
    {
      // var buildRow = @ptrCast(*BuildRow, @htEntryIterGetRow())
      function->Append(
          codegen->DeclareVarWithInit(build_row_var_, codegen->HTEntryIterGetRow(entry_iter, build_row_type_)));
      CheckJoinPredicate(ctx, function);
    }
    entry_loop.EndLoop();
  }
}

void HashJoinTranslator::CheckJoinPredicate(WorkContext *ctx, FunctionBuilder *function) const {
  const auto &join_plan = GetPlanAs<planner::HashJoinPlanNode>();
  auto *codegen = GetCodeGen();

  auto cond = ctx->DeriveValue(*join_plan.GetJoinPredicate(), this);
  if (join_plan.GetLogicalJoinType() == planner::LogicalJoinType::LEFT_SEMI) {
    // For left-semi joins, we also need to make sure the build-side tuple
    // has not already found an earlier join partner. We enforce the check
    // by modifying the join predicate.
    auto left_mark = codegen->AccessStructMember(codegen->MakeExpr(build_row_var_), build_mark_);
    cond = codegen->BinaryOp(parsing::Token::Type::AND, left_mark, cond);
  }

  If check_condition(function, cond);
  {
    if (join_plan.RequiresLeftMark()) {
      // Mark this tuple as accessed.
      auto left_mark = codegen->AccessStructMember(codegen->MakeExpr(build_row_var_), build_mark_);
      function->Append(codegen->Assign(left_mark, codegen->ConstBool(false)));
    }

    // If left outer join, then call joinConsumer in order to reduce TPL code duplication,
    // otherwise just push to parent
    if (join_plan.GetLogicalJoinType() == planner::LogicalJoinType::LEFT) {
      // var probeRow : ProbeRow
      auto probe_row_type = codegen->MakeExpr(probe_row_type_);
      auto probe_row = codegen->MakeExpr(probe_row_var_);
      function->Append(codegen->DeclareVarNoInit(probe_row_var_, probe_row_type));
      // Fill row.
      FillProbeRow(ctx, function, codegen->MakeExpr(probe_row_var_));
      // joinConsumer(queryState, pipelineState, buildRow, probeRow);
      std::initializer_list<ast::Expr *> args{GetQueryStatePtr(),
                                              codegen->MakeExpr(GetPipeline()->GetPipelineStateVar()),
                                              codegen->MakeExpr(build_row_var_), codegen->AddressOf(probe_row)};
      function->Append(codegen->Call(join_consumer_, args));
    } else {
      // Just push forward
      ctx->Push(function);
    }

    CounterAdd(function, num_match_rows_, 1);
  }

  check_condition.EndIf();
}

void HashJoinTranslator::CheckRightMark(WorkContext *ctx, FunctionBuilder *function, ast::Identifier right_mark) const {
  auto *codegen = GetCodeGen();

  // Generate the join condition.
  const auto join_predicate = GetPlanAs<planner::HashJoinPlanNode>().GetJoinPredicate();
  auto cond = ctx->DeriveValue(*join_predicate, this);

  If check_condition(function, cond);
  {
    // If there is a match, unset the right mark now.
    function->Append(codegen->Assign(codegen->MakeExpr(right_mark), codegen->ConstBool(false)));
  }
  check_condition.EndIf();
}

void HashJoinTranslator::CollectUnmatchedLeftRows(FunctionBuilder *function) const {
  auto *codegen = GetCodeGen();

  // var joinHTIterBase: JoinHashTableIterator
  auto jht_iter_base = codegen->MakeFreshIdentifier("joinHTIterBase");
  auto jht_iter_type = codegen->BuiltinType(ast::BuiltinType::JoinHashTableIterator);
  function->Append(codegen->DeclareVarNoInit(jht_iter_base, jht_iter_type));

  // var joinHTIter = &joinHTIterBase
  auto jht_iter = codegen->MakeFreshIdentifier("joinHTIter");
  auto jht_iter_init = codegen->AddressOf(codegen->MakeExpr(jht_iter_base));
  function->Append(codegen->DeclareVarWithInit(jht_iter, jht_iter_init));

  // while (hasNext()):
  auto jht = global_join_ht_.GetPtr(codegen);
  auto jht_iter_expr = codegen->MakeExpr(jht_iter);
  Loop loop(function, codegen->MakeStmt(codegen->JoinHTIteratorInit(jht_iter_expr, jht)),
            codegen->JoinHTIteratorHasNext(jht_iter_expr),
            codegen->MakeStmt(codegen->JoinHTIteratorNext(jht_iter_expr)));
  {
    // var buildRow = @joinHTIterGetRow()
    function->Append(
        codegen->DeclareVarWithInit(build_row_var_, codegen->JoinHTIteratorGetRow(jht_iter_expr, build_row_type_)));

    auto left_mark = codegen->AccessStructMember(codegen->MakeExpr(build_row_var_), build_mark_);

    // If mark is true, then row was not matched
    If check_condition(function, left_mark);
    {
      // var probeRow : ProbeRow
      auto probe_row_type = codegen->MakeExpr(probe_row_type_);
      auto probe_row = codegen->MakeExpr(probe_row_var_);
      function->Append(codegen->DeclareVarNoInit(probe_row_var_, probe_row_type));
      // Fill probe row with NULLs
      const auto child_schema = GetPlan().GetChild(1)->GetOutputSchema();
      for (uint32_t attr_idx = 0; attr_idx < child_schema->GetColumns().size(); attr_idx++) {
        auto type = child_schema->GetColumn(attr_idx).GetType();
        ast::Expr *lhs = GetRowAttribute(probe_row, attr_idx);
        ast::Expr *rhs = GetCodeGen()->ConstNull(type);
        function->Append(codegen->Assign(lhs, rhs));
      }
      // joinConsumer(queryState, pipelineState, buildRow, probeRow);
      std::initializer_list<ast::Expr *> args{GetQueryStatePtr(),
                                              codegen->MakeExpr(GetPipeline()->GetPipelineStateVar()),
                                              codegen->MakeExpr(build_row_var_), codegen->AddressOf(probe_row)};
      function->Append(codegen->Call(join_consumer_, args));
    }
  }
  loop.EndLoop();

  // Close iterator.
  function->Append(codegen->JoinHTIteratorFree(jht_iter_expr));
}

void HashJoinTranslator::PerformPipelineWork(WorkContext *ctx, FunctionBuilder *function) const {
  if (IsLeftPipeline(ctx->GetPipeline())) {
    InsertIntoJoinHashTable(ctx, function);
  } else {
    NOISEPAGE_ASSERT(IsRightPipeline(ctx->GetPipeline()), "Pipeline is unknown to join translator");
    ProbeJoinHashTable(ctx, function);
  }
}

void HashJoinTranslator::FinishPipelineWork(const Pipeline &pipeline, FunctionBuilder *function) const {
  auto *codegen = GetCodeGen();

  if (IsLeftPipeline(pipeline)) {
    ast::Expr *jht = global_join_ht_.GetPtr(codegen);

    if (left_pipeline_.IsParallel()) {
      if (IsPipelineMetricsEnabled()) {
        // Setup the hooks
        auto *exec_ctx = GetExecutionContext();
        auto num_hooks = static_cast<uint32_t>(sql::JoinHashTable::HookOffsets::NUM_HOOKS);
        auto pre = static_cast<uint32_t>(sql::JoinHashTable::HookOffsets::StartHook);
        auto post = static_cast<uint32_t>(sql::JoinHashTable::HookOffsets::EndHook);
        function->Append(codegen->ExecCtxInitHooks(exec_ctx, num_hooks));
        function->Append(codegen->ExecCtxRegisterHook(exec_ctx, pre, parallel_build_pre_hook_fn_));
        function->Append(codegen->ExecCtxRegisterHook(exec_ctx, post, parallel_build_post_hook_fn_));
      }

      auto *tls = GetThreadStateContainer();
      auto *offset = local_join_ht_.OffsetFromState(codegen);
      function->Append(codegen->JoinHashTableBuildParallel(jht, tls, offset));

      if (IsPipelineMetricsEnabled()) {
        auto *exec_ctx = GetExecutionContext();
        function->Append(codegen->ExecCtxClearHooks(exec_ctx));
      }
    } else {
      function->Append(codegen->JoinHashTableBuild(jht));
      RecordCounters(pipeline, function);
    }
  } else {
    if (GetPlanAs<planner::HashJoinPlanNode>().GetLogicalJoinType() == planner::LogicalJoinType::LEFT) {
      CollectUnmatchedLeftRows(function);
    }

    if (!pipeline.IsParallel()) {
      RecordCounters(pipeline, function);
    }
  }
}

ast::Expr *HashJoinTranslator::GetChildOutput(WorkContext *context, uint32_t child_idx, uint32_t attr_idx) const {
  // If the request is in the probe pipeline and for an attribute in the left
  // child, we read it from the probe/materialized build row.
  //
  // Otherwise if within the joinConsumer function we read from the ProbeRow and if not propagate
  // the request to the correct child
  if (IsRightPipeline(context->GetPipeline()) && child_idx == 0) {
    auto row = GetCodeGen()->MakeExpr(build_row_var_);
    return GetRowAttribute(row, attr_idx);
  }
  if (IsRightPipeline(context->GetPipeline()) && child_idx == 1 && join_consumer_flag_) {
    auto row = GetCodeGen()->MakeExpr(probe_row_var_);
    return GetRowAttribute(row, attr_idx);
  }
  return OperatorTranslator::GetChildOutput(context, child_idx, attr_idx);
}

}  // namespace noisepage::execution::compiler
