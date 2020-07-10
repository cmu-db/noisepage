#include "execution/compiler/operator/operator_translator.h"

#include "common/error/exception.h"
#include "common/json.h"
#include "execution/compiler/compilation_context.h"
#include "execution/compiler/work_context.h"
#include "planner/plannodes/abstract_plan_node.h"
#include "spdlog/fmt/fmt.h"

namespace terrier::execution::compiler {

OperatorTranslator::OperatorTranslator(const planner::AbstractPlanNode &plan, CompilationContext *compilation_context,
                                       Pipeline *pipeline, brain::ExecutionOperatingUnitType feature_type)
    : plan_(plan), compilation_context_(compilation_context), pipeline_(pipeline), feature_type_(feature_type) {
  TERRIER_ASSERT(plan.GetOutputSchema() != nullptr, "Output schema shouldn't be null");
  // Register this operator.
  pipeline->RegisterStep(this);
  // Prepare all output expressions.
  for (const auto &output_column : plan.GetOutputSchema()->GetColumns()) {
    compilation_context->Prepare(*output_column.GetExpr());
  }
}

ast::Expr *OperatorTranslator::GetOutput(WorkContext *context, uint32_t attr_idx) const {
  // Check valid output column.
  const auto output_schema = plan_.GetOutputSchema();
  if (attr_idx >= output_schema->NumColumns()) {
    throw EXECUTION_EXCEPTION(fmt::format("Codegen: Cannot read column {} from '{}' with output schema {}", attr_idx,
                                          planner::PlanNodeTypeToString(plan_.GetPlanNodeType()),
                                          output_schema->ToJson().dump()));
  }

  const auto output_expression = output_schema->GetColumn(attr_idx).GetExpr();
  return context->DeriveValue(*output_expression, this);
}

ast::Expr *OperatorTranslator::GetChildOutput(WorkContext *context, uint32_t child_idx, uint32_t attr_idx) const {
  // Check valid child.
  if (child_idx >= plan_.GetChildrenSize()) {
    throw EXECUTION_EXCEPTION(fmt::format("Codegen: Plan type '{}' does not have child at index {}",
                                          planner::PlanNodeTypeToString(plan_.GetPlanNodeType()), child_idx));
  }

  // Check valid output column from child.
  auto child_translator = compilation_context_->LookupTranslator(*plan_.GetChild(child_idx));
  TERRIER_ASSERT(child_translator != nullptr, "Missing translator for child!");
  return child_translator->GetOutput(context, attr_idx);
}

CodeGen *OperatorTranslator::GetCodeGen() const { return compilation_context_->GetCodeGen(); }

ast::Expr *OperatorTranslator::GetQueryStatePtr() const {
  return compilation_context_->GetQueryState()->GetStatePointer(GetCodeGen());
}

ast::Expr *OperatorTranslator::GetExecutionContext() const {
  return compilation_context_->GetExecutionContextPtrFromQueryState();
}

ast::Expr *OperatorTranslator::GetThreadStateContainer() const {
  return GetCodeGen()->ExecCtxGetTLS(GetExecutionContext());
}

ast::Expr *OperatorTranslator::GetMemoryPool() const {
  return GetCodeGen()->ExecCtxGetMemoryPool(GetExecutionContext());
}

void OperatorTranslator::GetAllChildOutputFields(const uint32_t child_index, const std::string &field_name_prefix,
                                                 util::RegionVector<ast::FieldDecl *> *fields) const {
  auto *codegen = GetCodeGen();

  // Reserve now to reduce allocations.
  const auto child_output_schema = plan_.GetChild(child_index)->GetOutputSchema();
  fields->reserve(child_output_schema->NumColumns());

  // Add columns to output.
  uint32_t attr_idx = 0;
  for (const auto &col : plan_.GetChild(child_index)->GetOutputSchema()->GetColumns()) {
    auto field_name = codegen->MakeIdentifier(field_name_prefix + std::to_string(attr_idx++));
    auto type = codegen->TplType(sql::GetTypeId(col.GetExpr()->GetReturnValueType()));
    fields->emplace_back(codegen->MakeField(field_name, type));
  }
}

OperatorTranslator *OperatorTranslator::LookupPreparedChildTranslator(const planner::AbstractPlanNode &plan) const {
  return compilation_context_->LookupTranslator(plan);
}

}  // namespace terrier::execution::compiler
