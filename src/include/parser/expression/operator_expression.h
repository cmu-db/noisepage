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
  std::unique_ptr<AbstractExpression> Copy() const override;

  /**
   * Creates a copy of the current AbstractExpression with new children implanted.
   * The children should not be owned by any other AbstractExpression.
   * @param children New children to be owned by the copy
   * @returns copy of this
   */
  std::unique_ptr<AbstractExpression> CopyWithChildren(
      std::vector<std::unique_ptr<AbstractExpression>> &&children) const override;

  void DeriveReturnValueType() override;

  void Accept(common::ManagedPointer<binder::SqlNodeVisitor> v) override { v->Visit(common::ManagedPointer(this)); }
};

DEFINE_JSON_HEADER_DECLARATIONS(OperatorExpression);

}  // namespace terrier::parser
