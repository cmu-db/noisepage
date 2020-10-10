#include "execution/compiler/operator/hash_join_translator.h"

#include "execution/ast/type.h"
#include "execution/compiler/compilation_context.h"
#include "execution/compiler/function_builder.h"
#include "execution/compiler/if.h"
#include "execution/compiler/loop.h"
#include "execution/compiler/work_context.h"
#include "planner/plannodes/hash_join_plan_node.h"

namespace terrier::execution::compiler {

namespace {
const char *build_row_attr_prefix = "attr";
}  // namespace

HashJoinTranslator::HashJoinTranslator(const planner::HashJoinPlanNode &plan, CompilationContext *compilation_context,
                                       Pipeline *pipeline)
    // The ExecutionOperatingUnitType depends on whether it is the build pipeline or probe pipeline.
    : OperatorTranslator(plan, compilation_context, pipeline, brain::ExecutionOperatingUnitType::DUMMY),
      build_row_var_(GetCodeGen()->MakeFreshIdentifier("buildRow")),
      build_row_type_(GetCodeGen()->MakeFreshIdentifier("BuildRow")),
      build_mark_(GetCodeGen()->MakeFreshIdentifier("buildMark")),
      left_pipeline_(this, Pipeline::Parallelism::Parallel) {
  TERRIER_ASSERT(!plan.GetLeftHashKeys().empty(), "Hash-join must have join keys from left input");
  TERRIER_ASSERT(!plan.GetRightHashKeys().empty(), "Hash-join must have join keys from right input");
  TERRIER_ASSERT(plan.GetJoinPredicate() != nullptr, "Hash-join must have a join predicate!");

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

  num_build_rows_ = CounterDeclare("num_build_rows");
  num_probe_rows_ = CounterDeclare("num_probe_rows");
  num_match_rows_ = CounterDeclare("num_match_rows");
}

void HashJoinTranslator::DefineHelperStructs(util::RegionVector<ast::StructDecl *> *decls) {
  auto *codegen = GetCodeGen();
  auto fields = codegen->MakeEmptyFieldList();
  GetAllChildOutputFields(0, build_row_attr_prefix, &fields);
  if (GetPlanAs<planner::HashJoinPlanNode>().RequiresLeftMark()) {
    fields.push_back(codegen->MakeField(build_mark_, codegen->BoolType()));
  }
  ast::StructDecl *struct_decl = codegen->DeclareStruct(build_row_type_, std::move(fields));
  struct_decl_ = struct_decl;
  decls->push_back(struct_decl);
}

void HashJoinTranslator::InitializeJoinHashTable(FunctionBuilder *function, ast::Expr *jht_ptr) const {
  function->Append(GetCodeGen()->JoinHashTableInit(jht_ptr, GetExecutionContext(), GetMemoryPool(), build_row_type_));
}

void HashJoinTranslator::TearDownJoinHashTable(FunctionBuilder *function, ast::Expr *jht_ptr) const {
  function->Append(GetCodeGen()->JoinHashTableFree(jht_ptr));
}

void HashJoinTranslator::InitializeQueryState(FunctionBuilder *function) const {
  auto *codegen = GetCodeGen();
  InitializeJoinHashTable(function, global_join_ht_.GetPtr(codegen));

  CounterSet(function, num_build_rows_, 0);
  CounterSet(function, num_probe_rows_, 0);
  CounterSet(function, num_match_rows_, 0);
}

void HashJoinTranslator::TearDownQueryState(FunctionBuilder *function) const {
  TearDownJoinHashTable(function, global_join_ht_.GetPtr(GetCodeGen()));
}

void HashJoinTranslator::InitializePipelineState(const Pipeline &pipeline, FunctionBuilder *function) const {
  if (IsLeftPipeline(pipeline) && left_pipeline_.IsParallel()) {
    InitializeJoinHashTable(function, local_join_ht_.GetPtr(GetCodeGen()));
  }
}

void HashJoinTranslator::TearDownPipelineState(const Pipeline &pipeline, FunctionBuilder *function) const {
  if (IsLeftPipeline(pipeline) && left_pipeline_.IsParallel()) {
    TearDownJoinHashTable(function, local_join_ht_.GetPtr(GetCodeGen()));
  }
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

ast::Expr *HashJoinTranslator::GetBuildRowAttribute(ast::Expr *build_row, uint32_t attr_idx) const {
  auto *codegen = GetCodeGen();
  auto attr_name = codegen->MakeIdentifier(build_row_attr_prefix + std::to_string(attr_idx));
  return codegen->AccessStructMember(build_row, attr_name);
}

void HashJoinTranslator::FillBuildRow(WorkContext *ctx, FunctionBuilder *function, ast::Expr *build_row) const {
  auto *codegen = GetCodeGen();
  const auto child_schema = GetPlan().GetChild(0)->GetOutputSchema();
  for (uint32_t attr_idx = 0; attr_idx < child_schema->GetColumns().size(); attr_idx++) {
    ast::Expr *lhs = GetBuildRowAttribute(build_row, attr_idx);
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
  if (join_plan.RequiresLeftMark()) {
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
    // Move along.
    ctx->Push(function);

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

void HashJoinTranslator::PerformPipelineWork(WorkContext *ctx, FunctionBuilder *function) const {
  if (IsLeftPipeline(ctx->GetPipeline())) {
    InsertIntoJoinHashTable(ctx, function);
  } else {
    TERRIER_ASSERT(IsRightPipeline(ctx->GetPipeline()), "Pipeline is unknown to join translator");
    ProbeJoinHashTable(ctx, function);
  }
}

void HashJoinTranslator::FinishPipelineWork(const Pipeline &pipeline, FunctionBuilder *function) const {
  auto *codegen = GetCodeGen();

  if (IsLeftPipeline(pipeline)) {
    ast::Expr *jht = global_join_ht_.GetPtr(codegen);

    if (left_pipeline_.IsParallel()) {
      auto *tls = GetThreadStateContainer();
      auto *offset = local_join_ht_.OffsetFromState(codegen);
      function->Append(codegen->JoinHashTableBuildParallel(jht, tls, offset));
    } else {
      function->Append(codegen->JoinHashTableBuild(jht));
    }

    FeatureRecord(function, brain::ExecutionOperatingUnitType::HASHJOIN_BUILD,
                  brain::ExecutionOperatingUnitFeatureAttribute::NUM_ROWS, pipeline, CounterVal(num_build_rows_));
    FeatureRecord(function, brain::ExecutionOperatingUnitType::HASHJOIN_BUILD,
                  brain::ExecutionOperatingUnitFeatureAttribute::CARDINALITY, pipeline,
                  codegen->CallBuiltin(ast::Builtin::JoinHashTableGetTupleCount, {jht}));
    FeatureArithmeticRecordMul(function, pipeline, GetTranslatorId(), CounterVal(num_build_rows_));
  } else {
    FeatureRecord(function, brain::ExecutionOperatingUnitType::HASHJOIN_PROBE,
                  brain::ExecutionOperatingUnitFeatureAttribute::NUM_ROWS, pipeline, CounterVal(num_probe_rows_));
    FeatureRecord(function, brain::ExecutionOperatingUnitType::HASHJOIN_PROBE,
                  brain::ExecutionOperatingUnitFeatureAttribute::CARDINALITY, pipeline, CounterVal(num_match_rows_));
    FeatureArithmeticRecordSet(function, pipeline, GetTranslatorId(), CounterVal(num_match_rows_));
  }
}

ast::Expr *HashJoinTranslator::GetChildOutput(WorkContext *context, uint32_t child_idx, uint32_t attr_idx) const {
  // If the request is in the probe pipeline and for an attribute in the left
  // child, we read it from the probe/materialized build row. Otherwise, we
  // propagate to the appropriate child.
  if (IsRightPipeline(context->GetPipeline()) && child_idx == 0) {
    return GetBuildRowAttribute(GetCodeGen()->MakeExpr(build_row_var_), attr_idx);
  }
  return OperatorTranslator::GetChildOutput(context, child_idx, attr_idx);
}

}  // namespace terrier::execution::compiler
