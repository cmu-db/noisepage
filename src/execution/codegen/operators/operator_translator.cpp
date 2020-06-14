#include "execution/sql/codegen/operators/operator_translator.h"

#include "spdlog/fmt/fmt.h"

#include "common/exception.h"
#include "execution/sql/codegen/compilation_context.h"
#include "execution/sql/codegen/work_context.h"
#include "execution/sql/planner/plannodes/abstract_plan_node.h"

namespace terrier::execution::codegen {

OperatorTranslator::OperatorTranslator(const planner::AbstractPlanNode &plan, CompilationContext *compilation_context,
                                       Pipeline *pipeline)
    : plan_(plan), compilation_context_(compilation_context), pipeline_(pipeline) {
  TPL_ASSERT(plan.GetOutputSchema() != nullptr, "Output schema shouldn't be null");
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
    throw Exception(ExceptionType::CodeGen,
                    fmt::format("Cannot read column {} from '{}' with output schema {}", attr_idx,
                                planner::PlanNodeTypeToString(plan_.GetPlanNodeType()), output_schema->ToString()));
  }

  const auto output_expression = output_schema->GetColumn(attr_idx).GetExpr();
  return context->DeriveValue(*output_expression, this);
}

ast::Expr *OperatorTranslator::GetChildOutput(WorkContext *context, uint32_t child_idx, uint32_t attr_idx) const {
  // Check valid child.
  if (child_idx >= plan_.GetChildrenSize()) {
    throw Exception(ExceptionType::CodeGen,
                    fmt::format("Plan type '{}' does not have child at index {}",
                                planner::PlanNodeTypeToString(plan_.GetPlanNodeType()), child_idx));
  }

  // Check valid output column from child.
  auto child_translator = compilation_context_->LookupTranslator(*plan_.GetChild(child_idx));
  TPL_ASSERT(child_translator != nullptr, "Missing translator for child!");
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
  CodeGen *codegen = GetCodeGen();

  // Reserve now to reduce allocations.
  const auto child_output_schema = plan_.GetChild(child_index)->GetOutputSchema();
  fields->reserve(child_output_schema->NumColumns());

  // Add columns to output.
  uint32_t attr_idx = 0;
  for (const auto &col : plan_.GetChild(child_index)->GetOutputSchema()->GetColumns()) {
    auto field_name = codegen->MakeIdentifier(field_name_prefix + std::to_string(attr_idx++));
    auto type = codegen->TplType(col.GetExpr()->GetReturnValueType());
    fields->emplace_back(codegen->MakeField(field_name, type));
  }
}

}  // namespace terrier::execution::codegen
