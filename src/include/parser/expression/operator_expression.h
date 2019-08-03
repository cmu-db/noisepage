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
                     std::vector<const AbstractExpression *> children)
      : AbstractExpression(expression_type, return_value_type, std::move(children)) {}

  /**
   * Default constructor for deserialization
   */
  OperatorExpression() = default;

  ~OperatorExpression() override = default;

  /**
   * Copies this OperatorExpression
   * @returns copy of this
   */
  const AbstractExpression *Copy() const override {
    std::vector<const AbstractExpression *> children;
    for (const auto *child : children_) {
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
  const AbstractExpression *CopyWithChildren(std::vector<const AbstractExpression *> children) const override {
    return new OperatorExpression(*this, std::move(children));
  }

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
    auto children = children_;
    auto max_type_child = std::max_element(children.begin(), children.end(), [](auto t1, auto t2) {
      return t1->GetReturnValueType() < t2->GetReturnValueType();
    });
    auto type = (*max_type_child)->GetReturnValueType();
    TERRIER_ASSERT(type <= type::TypeId::DECIMAL, "Invalid operand type in Operator Expression.");
    this->SetReturnValueType(type);
  }

  void Accept(SqlNodeVisitor *v) override { v->Visit(this); }

 private:
  /**
   * Copy constructor for OperatorExpression
   * Relies on AbstractExpression copy constructor for base members
   * @param other Other OperatorExpression to copy from
   * @param children new OperatorExpression's children
   */
  OperatorExpression(const OperatorExpression &other, std::vector<const AbstractExpression *> &&children)
      : AbstractExpression(other) {
    children_ = children;
  }
};

DEFINE_JSON_DECLARATIONS(OperatorExpression);

}  // namespace terrier::parser
