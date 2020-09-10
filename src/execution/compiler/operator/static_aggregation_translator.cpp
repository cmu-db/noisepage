#include "execution/compiler/operator/static_aggregation_translator.h"

#include "execution/compiler/compilation_context.h"
#include "execution/compiler/function_builder.h"
#include "execution/compiler/if.h"
#include "execution/compiler/work_context.h"
#include "planner/plannodes/aggregate_plan_node.h"

namespace terrier::execution::compiler {

namespace {
constexpr char AGG_ATTR_PREFIX[] = "agg_term_attr";
}  // namespace

StaticAggregationTranslator::StaticAggregationTranslator(const planner::AggregatePlanNode &plan,
                                                         CompilationContext *compilation_context, Pipeline *pipeline)
    : OperatorTranslator(plan, compilation_context, pipeline, brain::ExecutionOperatingUnitType::STATIC_AGGREGATE),
      agg_row_var_(GetCodeGen()->MakeFreshIdentifier("aggRow")),
      agg_payload_type_(GetCodeGen()->MakeFreshIdentifier("AggPayload")),
      agg_values_type_(GetCodeGen()->MakeFreshIdentifier("AggValues")),
      merge_func_(GetCodeGen()->MakeFreshIdentifier("MergeAggregates")),
      build_pipeline_(this, Pipeline::Parallelism::Parallel) {
  TERRIER_ASSERT(plan.GetGroupByTerms().empty(), "Global aggregations shouldn't have grouping keys");
  TERRIER_ASSERT(plan.GetChildrenSize() == 1, "Global aggregations should only have one child");
  build_pipeline_.UpdateParallelism(Pipeline::Parallelism::Serial);

  // The produce-side is serial since it only generates one output tuple.
  pipeline->RegisterSource(this, Pipeline::Parallelism::Serial);

  // The produce-side begins after the build-side.
  pipeline->LinkSourcePipeline(&build_pipeline_);

  // Prepare the child.
  compilation_context->Prepare(*plan.GetChild(0), &build_pipeline_);

  // Prepare each of the aggregate expressions.
  for (const auto agg_term : plan.GetAggregateTerms()) {
    compilation_context->Prepare(*agg_term->GetChild(0));
  }

  // If there's a having clause, prepare it, too.
  if (const auto having_clause = plan.GetHavingClausePredicate(); having_clause != nullptr) {
    compilation_context->Prepare(*having_clause);
  }

  auto *codegen = GetCodeGen();
  ast::Expr *payload_type = codegen->MakeExpr(agg_payload_type_);
  global_aggs_ = compilation_context->GetQueryState()->DeclareStateEntry(codegen, "aggs", payload_type);

  if (build_pipeline_.IsParallel()) {
    local_aggs_ = build_pipeline_.DeclarePipelineStateEntry("aggs", payload_type);
  }
}

ast::StructDecl *StaticAggregationTranslator::GeneratePayloadStruct() {
  auto *codegen = GetCodeGen();
  auto fields = codegen->MakeEmptyFieldList();
  fields.reserve(GetAggPlan().GetAggregateTerms().size());

  uint32_t term_idx = 0;
  for (const auto &term : GetAggPlan().GetAggregateTerms()) {
    auto name = codegen->MakeIdentifier(AGG_ATTR_PREFIX + std::to_string(term_idx++));
    auto type = codegen->AggregateType(term->GetExpressionType(), sql::GetTypeId(term->GetReturnValueType()));
    fields.push_back(codegen->MakeField(name, type));
  }

  struct_decl_ = codegen->DeclareStruct(agg_payload_type_, std::move(fields));
  return struct_decl_;
}

ast::StructDecl *StaticAggregationTranslator::GenerateValuesStruct() {
  auto *codegen = GetCodeGen();
  auto fields = codegen->MakeEmptyFieldList();
  fields.reserve(GetAggPlan().GetAggregateTerms().size());

  uint32_t term_idx = 0;
  for (const auto &term : GetAggPlan().GetAggregateTerms()) {
    TERRIER_ASSERT(term->GetChild(0)->GetReturnValueType() != type::TypeId::INVALID,
                   "Return value type of child expression is invalid.");
    auto field_name = codegen->MakeIdentifier(AGG_ATTR_PREFIX + std::to_string(term_idx));
    auto type = codegen->TplType(sql::GetTypeId(term->GetChild(0)->GetReturnValueType()));
    fields.push_back(codegen->MakeField(field_name, type));
    term_idx++;
  }
  return codegen->DeclareStruct(agg_values_type_, std::move(fields));
}

void StaticAggregationTranslator::DefineHelperStructs(util::RegionVector<ast::StructDecl *> *decls) {
  decls->push_back(GeneratePayloadStruct());

  decls->push_back(GenerateValuesStruct());
}

void StaticAggregationTranslator::DefineHelperFunctions(util::RegionVector<ast::FunctionDecl *> *decls) {
  if (build_pipeline_.IsParallel()) {
    auto *codegen = GetCodeGen();
    util::RegionVector<ast::FieldDecl *> params = build_pipeline_.PipelineParams();
    FunctionBuilder function(codegen, merge_func_, std::move(params), codegen->Nil());
    {
      for (uint32_t term_idx = 0; term_idx < GetAggPlan().GetAggregateTerms().size(); term_idx++) {
        auto lhs = GetAggregateTermPtr(global_aggs_.Get(codegen), term_idx);
        auto rhs = GetAggregateTermPtr(local_aggs_.Get(codegen), term_idx);
        function.Append(codegen->AggregatorMerge(lhs, rhs));
      }
    }
    decls->push_back(function.Finish());
  }
}

ast::Expr *StaticAggregationTranslator::GetAggregateTerm(ast::Expr *agg_row, uint32_t attr_idx) const {
  auto *codegen = GetCodeGen();
  auto member = codegen->MakeIdentifier(AGG_ATTR_PREFIX + std::to_string(attr_idx));
  return codegen->AccessStructMember(agg_row, member);
}

ast::Expr *StaticAggregationTranslator::GetAggregateTermPtr(ast::Expr *agg_row, uint32_t attr_idx) const {
  return GetCodeGen()->AddressOf(GetAggregateTerm(agg_row, attr_idx));
}

void StaticAggregationTranslator::InitializeAggregates(FunctionBuilder *function, bool local) const {
  auto *codegen = GetCodeGen();
  const auto aggs = local ? local_aggs_ : global_aggs_;
  for (uint32_t term_idx = 0; term_idx < GetAggPlan().GetAggregateTerms().size(); term_idx++) {
    ast::Expr *agg_term = GetAggregateTermPtr(aggs.Get(codegen), term_idx);
    function->Append(codegen->AggregatorInit(agg_term));
  }
}

void StaticAggregationTranslator::InitializePipelineState(const Pipeline &pipeline, FunctionBuilder *function) const {
  if (IsBuildPipeline(pipeline) && build_pipeline_.IsParallel()) {
    InitializeAggregates(function, true);
  }
}

void StaticAggregationTranslator::BeginPipelineWork(const Pipeline &pipeline, FunctionBuilder *function) const {
  if (IsBuildPipeline(pipeline)) {
    InitializeAggregates(function, false);
  }
}

void StaticAggregationTranslator::UpdateGlobalAggregate(WorkContext *ctx, FunctionBuilder *function) const {
  auto *codegen = GetCodeGen();

  const auto agg_payload = build_pipeline_.IsParallel() ? local_aggs_ : global_aggs_;

  // var aggValues: AggValues
  auto agg_values = codegen->MakeFreshIdentifier("aggValues");
  function->Append(codegen->DeclareVarNoInit(agg_values, codegen->MakeExpr(agg_values_type_)));

  // Fill values.
  uint32_t term_idx = 0;
  for (const auto &term : GetAggPlan().GetAggregateTerms()) {
    auto lhs = GetAggregateTerm(codegen->MakeExpr(agg_values), term_idx++);
    auto rhs = ctx->DeriveValue(*term->GetChild(0), this);
    function->Append(codegen->Assign(lhs, rhs));
  }

  // Update aggregate.
  for (term_idx = 0; term_idx < GetAggPlan().GetAggregateTerms().size(); term_idx++) {
    auto agg = GetAggregateTermPtr(agg_payload.Get(codegen), term_idx);
    auto val = GetAggregateTermPtr(codegen->MakeExpr(agg_values), term_idx);
    function->Append(codegen->AggregatorAdvance(agg, val));
  }
}

void StaticAggregationTranslator::PerformPipelineWork(WorkContext *context, FunctionBuilder *function) const {
  if (IsProducePipeline(context->GetPipeline())) {
    // var agg_row = &state.aggs
    auto *codegen = GetCodeGen();
    function->Append(codegen->DeclareVarWithInit(agg_row_var_, global_aggs_.GetPtr(codegen)));

    if (const auto having = GetAggPlan().GetHavingClausePredicate(); having != nullptr) {
      If check_having(function, context->DeriveValue(*having, this));
      context->Push(function);
      check_having.EndIf();
    } else {
      context->Push(function);
    }
  } else {
    UpdateGlobalAggregate(context, function);
  }
}

void StaticAggregationTranslator::FinishPipelineWork(const Pipeline &pipeline, FunctionBuilder *function) const {
  if (IsBuildPipeline(pipeline) && build_pipeline_.IsParallel()) {
    // Merge thread-local aggregates into one.
    auto *codegen = GetCodeGen();
    ast::Expr *thread_state_container = GetThreadStateContainer();
    ast::Expr *query_state = GetQueryStatePtr();
    function->Append(codegen->TLSIterate(thread_state_container, query_state, merge_func_));
  }
}

ast::Expr *StaticAggregationTranslator::GetChildOutput(WorkContext *context, UNUSED_ATTRIBUTE uint32_t child_idx,
                                                       uint32_t attr_idx) const {
  if (IsProducePipeline(context->GetPipeline())) {
    auto *codegen = GetCodeGen();
    return codegen->AggregatorResult(GetAggregateTermPtr(codegen->MakeExpr(agg_row_var_), attr_idx));
  }

  // The request is in the build pipeline. Forward to child translator.
  return OperatorTranslator::GetChildOutput(context, child_idx, attr_idx);
}

}  // namespace terrier::execution::compiler
