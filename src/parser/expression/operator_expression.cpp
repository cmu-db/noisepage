#include "parser/expression/operator_expression.h"

#include "common/json.h"

namespace terrier::parser {

std::unique_ptr<AbstractExpression> OperatorExpression::Copy() const {
  std::vector<std::unique_ptr<AbstractExpression>> children;
  for (const auto &child : GetChildren()) {
    children.emplace_back(child->Copy());
  }
  return CopyWithChildren(std::move(children));
}

std::unique_ptr<AbstractExpression> OperatorExpression::CopyWithChildren(
    std::vector<std::unique_ptr<AbstractExpression>> &&children) const {
  auto expr = std::make_unique<OperatorExpression>(GetExpressionType(), GetReturnValueType(), std::move(children));
  expr->SetMutableStateForCopy(*this);
  return expr;
}

void OperatorExpression::DeriveReturnValueType() {
  // if we are a decimal or int we should take the highest type id of both children
  // This relies on a particular order in expression_defs.h
  if (this->GetExpressionType() == ExpressionType::OPERATOR_NOT ||
      this->GetExpressionType() == ExpressionType::OPERATOR_IS_NULL ||
      this->GetExpressionType() == ExpressionType::OPERATOR_IS_NOT_NULL ||
      this->GetExpressionType() == ExpressionType::OPERATOR_EXISTS) {
    this->SetReturnValueType(type::TypeId::BOOLEAN);
    return;
  }
  const auto &children = this->GetChildren();
  const auto &max_type_child = std::max_element(children.begin(), children.end(), [](const auto &t1, const auto &t2) {
    return t1->GetReturnValueType() < t2->GetReturnValueType();
  });
  const auto &type = (*max_type_child)->GetReturnValueType();
  TERRIER_ASSERT(type <= type::TypeId::DECIMAL, "Invalid operand type in Operator Expression.");
  this->SetReturnValueType(type);
}

DEFINE_JSON_BODY_DECLARATIONS(OperatorExpression);

}  // namespace terrier::parser
