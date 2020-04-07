#pragma once

#include <memory>
#include <utility>
#include <vector>
#include "parser/expression/abstract_expression.h"

namespace terrier::parser {
/**
 * OperatorExpression represents a generic N-ary operator.
 */
class OperatorExpression : public AbstractExpression {
 public:
  /**
   * Instantiates a new operator.
   * @param expression_type type of operator
   * @param return_value_type return type of the operator
   * @param children vector containing arguments to the operator, left to right
   */
  OperatorExpression(const ExpressionType expression_type, const type::TypeId return_value_type,
                     std::vector<std::unique_ptr<AbstractExpression>> &&children)
      : AbstractExpression(expression_type, return_value_type, std::move(children)) {}

  /** Default constructor for deserialization. */
  OperatorExpression() = default;

  /**
   * Copies this OperatorExpression
   * @returns copy of this
   */
  std::unique_ptr<AbstractExpression> Copy() const override {
    std::vector<std::unique_ptr<AbstractExpression>> children;
    for (const auto &child : GetChildren()) {
      children.emplace_back(child->Copy());
    }
    return CopyWithChildren(std::move(children));
  }

  /**
   * Creates a copy of the current AbstractExpression with new children implanted.
   * The children should not be owned by any other AbstractExpression.
   * @param children New children to be owned by the copy
   * @returns copy of this
   */
  std::unique_ptr<AbstractExpression> CopyWithChildren(
      std::vector<std::unique_ptr<AbstractExpression>> &&children) const override {
    auto expr = std::make_unique<OperatorExpression>(GetExpressionType(), GetReturnValueType(), std::move(children));
    expr->SetMutableStateForCopy(*this);
    return expr;
  }

  void DeriveReturnValueType() override {
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

  void Accept(common::ManagedPointer<binder::SqlNodeVisitor> v) override { v->Visit(common::ManagedPointer(this)); }
};

DEFINE_JSON_DECLARATIONS(OperatorExpression);

}  // namespace terrier::parser
