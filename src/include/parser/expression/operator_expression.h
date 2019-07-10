#pragma once

#include <memory>
#include <utility>
#include <vector>
#include "parser/expression/abstract_expression.h"

namespace terrier::parser {

/**
 * Represents an operator.
 */
class OperatorExpression : public AbstractExpression {
 public:
  /**
   * Instantiates a new operator.
   * @param expression_type type of operator
   * @param return_value_type return type of the operator
   * @param children vector containing arguments to the operator left to right
   */
  OperatorExpression(const ExpressionType expression_type, const type::TypeId return_value_type,
                     std::vector<std::shared_ptr<AbstractExpression>> &&children)
      : AbstractExpression(expression_type, return_value_type, std::move(children)) {}

  /**
   * Default constructor for deserialization
   */
  OperatorExpression() = default;

  void DeriveReturnValueType() override {
    // if we are a decimal or int we should take the highest type id of both children
    // This relies on a particular order in types.h
    if (this->GetExpressionType() == ExpressionType::OPERATOR_NOT ||
        this->GetExpressionType() == ExpressionType::OPERATOR_IS_NULL ||
        this->GetExpressionType() == ExpressionType::OPERATOR_IS_NOT_NULL ||
        this->GetExpressionType() == ExpressionType::OPERATOR_EXISTS) {
      this->SetReturnValueType(type::TypeId::BOOLEAN);
      return;
    }
    auto children = this->GetChildren();
    auto max_type_child = std::max_element(children.begin(), children.end(), [](auto t1, auto t2) {
      return t1->GetReturnValueType() < t2->GetReturnValueType();
    });
    auto type = (*max_type_child)->GetReturnValueType();
    TERRIER_ASSERT(type <= type::TypeId::DECIMAL, "Invalid operand type in Operator Expression.");
    this->SetReturnValueType(type);
  }

  std::shared_ptr<AbstractExpression> Copy() const override { return std::make_shared<OperatorExpression>(*this); }

  void Accept(SqlNodeVisitor *v) override { v->Visit(this); }
};

DEFINE_JSON_DECLARATIONS(OperatorExpression);

}  // namespace terrier::parser
