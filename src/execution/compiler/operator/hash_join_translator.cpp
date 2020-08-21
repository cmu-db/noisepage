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
    : OperatorTranslator(plan, compilation_context, pipeline, brain::ExecutionOperatingUnitType::HASH_JOIN),
      left_outer_join_flag_(false),
      build_row_var_(GetCodeGen()->MakeFreshIdentifier("buildRow")),
      build_row_type_(GetCodeGen()->MakeFreshIdentifier("BuildRow")),
      build_mark_(GetCodeGen()->MakeFreshIdentifier("buildMark")),
      outer_join_consumer_(GetCodeGen()->MakeFreshIdentifier("outerJoinConsumer")),
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

void HashJoinTranslator::DefineHelperFunctions(util::RegionVector<ast::FunctionDecl *> *decls) {
  auto cc = GetCompilationContext();
  auto *pipeline = GetPipeline();
  // Create a WorkContext and make the state identical to the WorkContext generated inside
  // of PerformPipelineWork
  WorkContext ctx(cc, *pipeline);
  while (ctx.CurrentOp() != this) {
    ctx.AdvancePipelineIterator();
  }
  auto *codegen = GetCodeGen();
  util::RegionVector<ast::FieldDecl *> params = pipeline->PipelineParams();
  params.push_back(codegen->MakeField(build_row_var_, codegen->PointerType(build_row_type_)));
  auto join_type = GetPlanAs<planner::HashJoinPlanNode>().GetLogicalJoinType();
  // Set flag here, so GetChildOutput outputs NULL's for outer join correctly
  left_outer_join_flag_ = (join_type == planner::LogicalJoinType::LEFT);
  FunctionBuilder function(codegen, outer_join_consumer_, std::move(params), codegen->Nil());
  { ctx.Push(&function); }
  left_outer_join_flag_ = false;
  decls->push_back(function.Finish());
}

void HashJoinTranslator::InitializeJoinHashTable(FunctionBuilder *function, ast::Expr *jht_ptr) const {
  function->Append(GetCodeGen()->JoinHashTableInit(jht_ptr, GetExecutionContext(), GetMemoryPool(), build_row_type_));
}

void HashJoinTranslator::TearDownJoinHashTable(FunctionBuilder *function, ast::Expr *jht_ptr) const {
  function->Append(GetCodeGen()->JoinHashTableFree(jht_ptr));
}

void HashJoinTranslator::InitializeQueryState(FunctionBuilder *function) const {
  InitializeJoinHashTable(function, global_join_ht_.GetPtr(GetCodeGen()));
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
      right_anti_check.EndIf();
    } else if (join_plan.GetLogicalJoinType() == planner::LogicalJoinType::RIGHT_SEMI) {
      // If the right mark is unset, then there is at least one match.
      // if (!right_mark)
      auto cond = codegen->UnaryOp(parsing::Token::Type::BANG, codegen->MakeExpr(right_mark_var));
      If right_semi_check(function, cond);
      ctx->Push(function);
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
    // Move along.
    ctx->Push(function);
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

  // var naiveIterBase: HashTableNaiveIterator
  auto naive_iter_base = codegen->MakeFreshIdentifier("naiveIterBase");
  auto naive_iter_type = codegen->BuiltinType(ast::BuiltinType::HashTableNaiveIterator);
  function->Append(codegen->DeclareVarNoInit(naive_iter_base, naive_iter_type));

  // var naiveIter = &naiveIterBase
  auto naive_iter = codegen->MakeFreshIdentifier("naiveIter");
  auto naive_iter_init = codegen->AddressOf(codegen->MakeExpr(naive_iter_base));
  function->Append(codegen->DeclareVarWithInit(naive_iter, naive_iter_init));

  // while (hasNext()):
  auto join_ht = global_join_ht_.GetPtr(codegen);
  auto naive_iter_expr = codegen->MakeExpr(naive_iter);
  Loop loop(function, codegen->MakeStmt(codegen->HTNaiveIteratorInit(naive_iter_expr, join_ht)),
            codegen->HTNaiveIteratorHasNext(naive_iter_expr),
            codegen->MakeStmt(codegen->HTNaiveIteratorNext(naive_iter_expr)));
  {
    // var buildRow = @htNaiveIterGetRow()
    function->Append(
        codegen->DeclareVarWithInit(build_row_var_, codegen->HTNaiveIteratorGetRow(naive_iter_expr, build_row_type_)));

    auto left_mark = codegen->AccessStructMember(codegen->MakeExpr(build_row_var_), build_mark_);

    // If mark is true, then row was not matched
    If check_condition(function, left_mark);
    {
      // outerJoinConsumer(queryState, pipelineState, buildRow);
      std::initializer_list<ast::Expr *> args{GetQueryStatePtr(),
                                              codegen->MakeExpr(GetPipeline()->GetPipelineStateVar()),
                                              codegen->MakeExpr(build_row_var_)};
      function->Append(codegen->Call(outer_join_consumer_, args));
    }
  }
  loop.EndLoop();

  // Close iterator.
  function->Append(codegen->HTNaiveIteratorFree(naive_iter_expr));
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
  if (IsLeftPipeline(pipeline)) {
    auto *codegen = GetCodeGen();
    auto jht = global_join_ht_.GetPtr(codegen);
    if (left_pipeline_.IsParallel()) {
      auto tls = GetThreadStateContainer();
      auto offset = local_join_ht_.OffsetFromState(codegen);
      function->Append(codegen->JoinHashTableBuildParallel(jht, tls, offset));
    } else {
      function->Append(codegen->JoinHashTableBuild(jht));
    }
  } else {
    if (GetPlanAs<planner::HashJoinPlanNode>().GetLogicalJoinType() == planner::LogicalJoinType::LEFT) {
      CollectUnmatchedLeftRows(function);
    }
  }
}

ast::Expr *HashJoinTranslator::GetChildOutput(WorkContext *context, uint32_t child_idx, uint32_t attr_idx) const {
  // If the request is in the probe pipeline and for an attribute in the left
  // child, we read it from the probe/materialized build row.
  //
  // Otherwise we either output a NULL if needed for an outer join or propagate
  // the request to the correct child
  if (IsRightPipeline(context->GetPipeline()) && child_idx == 0) {
    auto row = GetCodeGen()->MakeExpr(build_row_var_);
    return GetBuildRowAttribute(row, attr_idx);
  }
  if (IsRightPipeline(context->GetPipeline()) && child_idx == 1 && left_outer_join_flag_) {
    auto schema = this->GetPlan().GetOutputSchema();
    auto type = schema->GetColumn(attr_idx).GetType();
    return GetCodeGen()->ConstNull(type);
  }
  return OperatorTranslator::GetChildOutput(context, child_idx, attr_idx);
}

}  // namespace terrier::execution::compiler
