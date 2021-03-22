#include "execution/compiler/compilation_context.h"

#include <algorithm>
#include <atomic>
#include <sstream>

#include "common/error/exception.h"
#include "common/macros.h"
#include "execution/ast/context.h"
#include "execution/compiler/codegen.h"
#include "execution/compiler/executable_query.h"
#include "execution/compiler/executable_query_builder.h"
#include "execution/compiler/expression/arithmetic_translator.h"
#include "execution/compiler/expression/column_value_translator.h"
#include "execution/compiler/expression/comparison_translator.h"
#include "execution/compiler/expression/conjunction_translator.h"
#include "execution/compiler/expression/constant_translator.h"
#include "execution/compiler/expression/derived_value_translator.h"
#include "execution/compiler/expression/function_translator.h"
#include "execution/compiler/expression/null_check_translator.h"
#include "execution/compiler/expression/param_value_translator.h"
#include "execution/compiler/expression/star_translator.h"
#include "execution/compiler/expression/unary_translator.h"
#include "execution/compiler/function_builder.h"
#include "execution/compiler/operator/analyze_translator.h"
#include "execution/compiler/operator/csv_scan_translator.h"
#include "execution/compiler/operator/delete_translator.h"
#include "execution/compiler/operator/hash_aggregation_translator.h"
#include "execution/compiler/operator/hash_join_translator.h"
#include "execution/compiler/operator/index_create_translator.h"
#include "execution/compiler/operator/index_join_translator.h"
#include "execution/compiler/operator/index_scan_translator.h"
#include "execution/compiler/operator/insert_translator.h"
#include "execution/compiler/operator/limit_translator.h"
#include "execution/compiler/operator/nested_loop_join_translator.h"
#include "execution/compiler/operator/operator_translator.h"
#include "execution/compiler/operator/output_translator.h"
#include "execution/compiler/operator/projection_translator.h"
#include "execution/compiler/operator/seq_scan_translator.h"
#include "execution/compiler/operator/sort_translator.h"
#include "execution/compiler/operator/static_aggregation_translator.h"
#include "execution/compiler/operator/update_translator.h"
#include "execution/compiler/pipeline.h"
#include "execution/exec/execution_settings.h"
#include "parser/expression/abstract_expression.h"
#include "parser/expression/column_value_expression.h"
#include "parser/expression/comparison_expression.h"
#include "parser/expression/conjunction_expression.h"
#include "parser/expression/constant_value_expression.h"
#include "parser/expression/derived_value_expression.h"
#include "parser/expression/function_expression.h"
#include "parser/expression/operator_expression.h"
#include "parser/expression/parameter_value_expression.h"
#include "parser/expression/star_expression.h"
#include "planner/plannodes/abstract_plan_node.h"
#include "planner/plannodes/aggregate_plan_node.h"
#include "planner/plannodes/create_index_plan_node.h"
#include "planner/plannodes/csv_scan_plan_node.h"
#include "planner/plannodes/delete_plan_node.h"
#include "planner/plannodes/hash_join_plan_node.h"
#include "planner/plannodes/index_join_plan_node.h"
#include "planner/plannodes/index_scan_plan_node.h"
#include "planner/plannodes/insert_plan_node.h"
#include "planner/plannodes/limit_plan_node.h"
#include "planner/plannodes/nested_loop_join_plan_node.h"
#include "planner/plannodes/order_by_plan_node.h"
#include "planner/plannodes/projection_plan_node.h"
#include "planner/plannodes/seq_scan_plan_node.h"
#include "planner/plannodes/set_op_plan_node.h"
#include "planner/plannodes/update_plan_node.h"
#include "self_driving/modeling/operating_unit_recorder.h"
#include "spdlog/fmt/fmt.h"

namespace noisepage::execution::compiler {

namespace {
// A unique ID generator used to generate globally unique TPL function names and keep track of query ID for minirunners.
std::atomic<uint32_t> unique_ids{0};
}  // namespace

CompilationContext::CompilationContext(ExecutableQuery *query, catalog::CatalogAccessor *accessor,
                                       const CompilationMode mode, const exec::ExecutionSettings &settings)
    : unique_id_(unique_ids++),
      query_(query),
      mode_(mode),
      codegen_(query_->GetContext(), accessor),
      query_state_var_(codegen_.MakeIdentifier("queryState")),
      query_state_type_(codegen_.MakeIdentifier("QueryState")),
      query_state_(query_state_type_, [this](CodeGen *codegen) { return codegen->MakeExpr(query_state_var_); }),
      counters_enabled_(settings.GetIsCountersEnabled()),
      pipeline_metrics_enabled_(settings.GetIsPipelineMetricsEnabled()) {}

ast::FunctionDecl *CompilationContext::GenerateInitFunction() {
  const auto name = codegen_.MakeIdentifier(GetFunctionPrefix() + "_Init");
  FunctionBuilder builder(&codegen_, name, QueryParams(), codegen_.Nil());
  {
    // Request new scope for the function.
    CodeGen::CodeScope code_scope(&codegen_);
    for (auto &[_, op] : ops_) {
      (void)_;
      op->InitializeQueryState(&builder);
    }
  }
  return builder.Finish();
}

ast::FunctionDecl *CompilationContext::GenerateTearDownFunction() {
  const auto name = codegen_.MakeIdentifier(GetFunctionPrefix() + "_TearDown");
  FunctionBuilder builder(&codegen_, name, QueryParams(), codegen_.Nil());
  {
    // Request new scope for the function.
    CodeGen::CodeScope code_scope(&codegen_);
    for (auto &[_, op] : ops_) {
      (void)_;
      op->TearDownQueryState(&builder);
    }
  }
  return builder.Finish();
}

void CompilationContext::GeneratePlan(const planner::AbstractPlanNode &plan) {
  exec_ctx_ =
      query_state_.DeclareStateEntry(GetCodeGen(), "execCtx", codegen_.PointerType(ast::BuiltinType::ExecutionContext));

  // Recursively prepare all translators for the query.
  Pipeline main_pipeline(this);
  if (plan.GetOutputSchema()->NumColumns() != 0) {
    PrepareOut(plan, &main_pipeline);
  } else {
    Prepare(plan, &main_pipeline);
  }
  query_state_.ConstructFinalType(&codegen_);

  // Collect top-level structures and declarations.
  util::RegionVector<ast::StructDecl *> top_level_structs(query_->GetContext()->GetRegion());
  util::RegionVector<ast::FunctionDecl *> top_level_funcs(query_->GetContext()->GetRegion());
  for (auto &[_, op] : ops_) {
    (void)_;
    op->DefineHelperStructs(&top_level_structs);
    op->DefineHelperFunctions(&top_level_funcs);
  }
  top_level_structs.push_back(query_state_.GetType());

  // All fragments.
  std::vector<std::unique_ptr<ExecutableQuery::Fragment>> fragments;

  // The main builder. The initialization and tear-down code go here. In
  // one-shot compilation, all query code goes here, too.
  ExecutableQueryFragmentBuilder main_builder(query_->GetContext());
  main_builder.DeclareAll(top_level_structs);
  main_builder.DeclareAll(top_level_funcs);
  main_builder.RegisterStep(GenerateInitFunction());

  // Generate each pipeline.
  std::vector<Pipeline *> execution_order;
  main_pipeline.CollectDependencies(&execution_order);
  for (auto *pipeline : execution_order) {
    // Extract and record the translators.
    // Pipelines require obtaining feature IDs, but features don't exist until translators are extracted.
    // Therefore translator extraction must happen before pipelines are generated.
    selfdriving::OperatingUnitRecorder recorder(common::ManagedPointer(codegen_.GetCatalogAccessor()),
                                                common::ManagedPointer(codegen_.GetAstContext()),
                                                common::ManagedPointer(pipeline), query_->GetQueryText());
    auto features = recorder.RecordTranslators(pipeline->GetTranslators());
    codegen_.GetPipelineOperatingUnits()->RecordOperatingUnit(pipeline->GetPipelineId(), std::move(features));

    pipeline->Prepare(query_->GetExecutionSettings());
    {
      util::RegionVector<ast::FunctionDecl *> pipeline_decls(query_->GetContext()->GetRegion());
      for (auto &[_, op] : ops_) {
        (void)_;
        op->DefineTLSDependentHelperFunctions(*pipeline, &pipeline_decls);
      }
      main_builder.DeclareAll(pipeline_decls);
    }
    pipeline->GeneratePipeline(&main_builder);
  }

  // Register the tear-down function.
  auto teardown = GenerateTearDownFunction();
  main_builder.RegisterStep(teardown);

  main_builder.AddTeardownFn(teardown);

  // Compile and finish.
  fragments.emplace_back(main_builder.Compile());
  query_->Setup(std::move(fragments), query_state_.GetSize(), codegen_.ReleasePipelineOperatingUnits());
}

// static
std::unique_ptr<ExecutableQuery> CompilationContext::Compile(const planner::AbstractPlanNode &plan,
                                                             const exec::ExecutionSettings &exec_settings,
                                                             catalog::CatalogAccessor *accessor,
                                                             const CompilationMode mode,
                                                             common::ManagedPointer<const std::string> query_text) {
  // The query we're generating code for.
  auto query = std::make_unique<ExecutableQuery>(plan, exec_settings);
  // TODO(Lin): Hacking... remove this after getting the counters in
  query->SetQueryText(query_text);

  // Generate the plan for the query
  CompilationContext ctx(query.get(), accessor, mode, exec_settings);
  ctx.GeneratePlan(plan);

  // Done
  return query;
}

uint32_t CompilationContext::RegisterPipeline(Pipeline *pipeline) {
  NOISEPAGE_ASSERT(std::find(pipelines_.begin(), pipelines_.end(), pipeline) == pipelines_.end(),
                   "Duplicate pipeline in context");
  pipelines_.push_back(pipeline);
  return pipelines_.size();
}

void CompilationContext::PrepareOut(const planner::AbstractPlanNode &plan, Pipeline *pipeline) {
  auto translator = std::make_unique<OutputTranslator>(plan, this, pipeline);
  ops_[nullptr] = std::move(translator);
}

void CompilationContext::Prepare(const planner::AbstractPlanNode &plan, Pipeline *pipeline) {
  std::unique_ptr<OperatorTranslator> translator;

  switch (plan.GetPlanNodeType()) {
    case planner::PlanNodeType::AGGREGATE: {
      const auto &aggregation = dynamic_cast<const planner::AggregatePlanNode &>(plan);
      if (aggregation.GetAggregateStrategyType() == planner::AggregateStrategyType::SORTED) {
        throw NOT_IMPLEMENTED_EXCEPTION("Code generation for sort-based aggregations.");
      }
      if (aggregation.IsStaticAggregation()) {
        translator = std::make_unique<StaticAggregationTranslator>(aggregation, this, pipeline);
      } else {
        translator = std::make_unique<HashAggregationTranslator>(aggregation, this, pipeline);
      }
      break;
    }
    case planner::PlanNodeType::CSVSCAN: {
      const auto &scan_plan = dynamic_cast<const planner::CSVScanPlanNode &>(plan);
      translator = std::make_unique<CSVScanTranslator>(scan_plan, this, pipeline);
      break;
    }
    case planner::PlanNodeType::HASHJOIN: {
      const auto &hash_join = dynamic_cast<const planner::HashJoinPlanNode &>(plan);
      translator = std::make_unique<HashJoinTranslator>(hash_join, this, pipeline);
      break;
    }
    case planner::PlanNodeType::LIMIT: {
      const auto &limit = dynamic_cast<const planner::LimitPlanNode &>(plan);
      translator = std::make_unique<LimitTranslator>(limit, this, pipeline);
      break;
    }
    case planner::PlanNodeType::NESTLOOP: {
      const auto &nested_loop = dynamic_cast<const planner::NestedLoopJoinPlanNode &>(plan);
      translator = std::make_unique<NestedLoopJoinTranslator>(nested_loop, this, pipeline);
      break;
    }
    case planner::PlanNodeType::ORDERBY: {
      const auto &sort = dynamic_cast<const planner::OrderByPlanNode &>(plan);
      translator = std::make_unique<SortTranslator>(sort, this, pipeline);
      break;
    }
    case planner::PlanNodeType::PROJECTION: {
      const auto &projection = dynamic_cast<const planner::ProjectionPlanNode &>(plan);
      translator = std::make_unique<ProjectionTranslator>(projection, this, pipeline);
      break;
    }
    case planner::PlanNodeType::SEQSCAN: {
      const auto &seq_scan = dynamic_cast<const planner::SeqScanPlanNode &>(plan);
      translator = std::make_unique<SeqScanTranslator>(seq_scan, this, pipeline);
      break;
    }
    case planner::PlanNodeType::INSERT: {
      const auto &insert = dynamic_cast<const planner::InsertPlanNode &>(plan);
      translator = std::make_unique<InsertTranslator>(insert, this, pipeline);
      break;
    }
    case planner::PlanNodeType::DELETE: {
      const auto &delete_node = dynamic_cast<const planner::DeletePlanNode &>(plan);
      translator = std::make_unique<DeleteTranslator>(delete_node, this, pipeline);
      break;
    }
    case planner::PlanNodeType::UPDATE: {
      const auto &update_node = dynamic_cast<const planner::UpdatePlanNode &>(plan);
      translator = std::make_unique<UpdateTranslator>(update_node, this, pipeline);
      break;
    }
    case planner::PlanNodeType::INDEXSCAN: {
      const auto &index_scan = dynamic_cast<const planner::IndexScanPlanNode &>(plan);
      translator = std::make_unique<IndexScanTranslator>(index_scan, this, pipeline);
      break;
    }
    case planner::PlanNodeType::INDEXNLJOIN: {
      const auto &index_join = dynamic_cast<const planner::IndexJoinPlanNode &>(plan);
      translator = std::make_unique<IndexJoinTranslator>(index_join, this, pipeline);
      break;
    }
    case planner::PlanNodeType::CREATE_INDEX: {
      const auto &create_index = dynamic_cast<const planner::CreateIndexPlanNode &>(plan);
      translator = std::make_unique<IndexCreateTranslator>(create_index, this, pipeline);
      break;
    }
    case planner::PlanNodeType::ANALYZE: {
      const auto &analyze = dynamic_cast<const planner::AnalyzePlanNode &>(plan);
      translator = std::make_unique<AnalyzeTranslator>(analyze, this, pipeline);
      break;
    }
    default: {
      throw NOT_IMPLEMENTED_EXCEPTION(fmt::format("code generation for plan node type '{}'",
                                                  planner::PlanNodeTypeToString(plan.GetPlanNodeType())));
    }
  }

  ops_[&plan] = std::move(translator);
}

void CompilationContext::Prepare(const parser::AbstractExpression &expression) {
  std::unique_ptr<ExpressionTranslator> translator;

  switch (expression.GetExpressionType()) {
    case parser::ExpressionType::COLUMN_VALUE: {
      const auto &column_value = dynamic_cast<const parser::ColumnValueExpression &>(expression);
      translator = std::make_unique<ColumnValueTranslator>(column_value, this);
      break;
    }
    case parser::ExpressionType::COMPARE_EQUAL:
    case parser::ExpressionType::COMPARE_GREATER_THAN:
    case parser::ExpressionType::COMPARE_GREATER_THAN_OR_EQUAL_TO:
    case parser::ExpressionType::COMPARE_LESS_THAN:
    case parser::ExpressionType::COMPARE_LESS_THAN_OR_EQUAL_TO:
    case parser::ExpressionType::COMPARE_NOT_EQUAL:
    case parser::ExpressionType::COMPARE_LIKE:
    case parser::ExpressionType::COMPARE_IN:
    case parser::ExpressionType::COMPARE_NOT_LIKE: {
      const auto &comparison = dynamic_cast<const parser::ComparisonExpression &>(expression);
      translator = std::make_unique<ComparisonTranslator>(comparison, this);
      break;
    }
    case parser::ExpressionType::CONJUNCTION_AND:
    case parser::ExpressionType::CONJUNCTION_OR: {
      const auto &conjunction = dynamic_cast<const parser::ConjunctionExpression &>(expression);
      translator = std::make_unique<ConjunctionTranslator>(conjunction, this);
      break;
    }
    case parser::ExpressionType::OPERATOR_PLUS:
    case parser::ExpressionType::OPERATOR_MINUS:
    case parser::ExpressionType::OPERATOR_MULTIPLY:
    case parser::ExpressionType::OPERATOR_DIVIDE:
    case parser::ExpressionType::OPERATOR_MOD: {
      const auto &operator_expr = dynamic_cast<const parser::OperatorExpression &>(expression);
      translator = std::make_unique<ArithmeticTranslator>(operator_expr, this);
      break;
    }
    case parser::ExpressionType::OPERATOR_NOT:
    case parser::ExpressionType::OPERATOR_UNARY_MINUS: {
      const auto &operator_expr = dynamic_cast<const parser::OperatorExpression &>(expression);
      translator = std::make_unique<UnaryTranslator>(operator_expr, this);
      break;
    }
    case parser::ExpressionType::OPERATOR_IS_NULL:
    case parser::ExpressionType::OPERATOR_IS_NOT_NULL: {
      const auto &operator_expr = dynamic_cast<const parser::OperatorExpression &>(expression);
      translator = std::make_unique<NullCheckTranslator>(operator_expr, this);
      break;
    }
    case parser::ExpressionType::VALUE_CONSTANT: {
      const auto &constant = dynamic_cast<const parser::ConstantValueExpression &>(expression);
      translator = std::make_unique<ConstantTranslator>(constant, this);
      break;
    }
    case parser::ExpressionType::VALUE_TUPLE: {
      const auto &derived_value = dynamic_cast<const parser::DerivedValueExpression &>(expression);
      translator = std::make_unique<DerivedValueTranslator>(derived_value, this);
      break;
    }
    case parser::ExpressionType::VALUE_PARAMETER: {
      const auto &param = dynamic_cast<const parser::ParameterValueExpression &>(expression);
      translator = std::make_unique<ParamValueTranslator>(param, this);
      break;
    }
    case parser::ExpressionType::STAR: {
      const auto &star = dynamic_cast<const parser::StarExpression &>(expression);
      translator = std::make_unique<StarTranslator>(star, this);
      break;
    }
    case parser::ExpressionType::FUNCTION: {
      const auto &fn = dynamic_cast<const parser::FunctionExpression &>(expression);
      translator = std::make_unique<FunctionTranslator>(fn, this);
      break;
    }
    default: {
      throw NOT_IMPLEMENTED_EXCEPTION(fmt::format("Code generation for expression type '{}' not supported.",
                                                  parser::ExpressionTypeToString(expression.GetExpressionType())));
    }
  }

  expressions_[&expression] = std::move(translator);
}

OperatorTranslator *CompilationContext::LookupTranslator(const planner::AbstractPlanNode &node) const {
  if (auto iter = ops_.find(&node); iter != ops_.end()) {
    return iter->second.get();
  }
  return nullptr;
}

ExpressionTranslator *CompilationContext::LookupTranslator(const parser::AbstractExpression &expr) const {
  if (auto iter = expressions_.find(&expr); iter != expressions_.end()) {
    return iter->second.get();
  }
  return nullptr;
}

std::string CompilationContext::GetFunctionPrefix() const { return "Query" + std::to_string(unique_id_); }

util::RegionVector<ast::FieldDecl *> CompilationContext::QueryParams() const {
  ast::Expr *state_type = codegen_.PointerType(codegen_.MakeExpr(query_state_type_));
  ast::FieldDecl *field = codegen_.MakeField(query_state_var_, state_type);
  return codegen_.MakeFieldList({field});
}

ast::Expr *CompilationContext::GetExecutionContextPtrFromQueryState() { return exec_ctx_.Get(&codegen_); }

}  // namespace noisepage::execution::compiler
