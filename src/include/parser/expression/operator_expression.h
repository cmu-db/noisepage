#pragma once

#include <vector>
#include "parser/expression/abstract_expression.h"
#include "parser/expression/expression_defs.h"
#include "type/value.h"

namespace terrier {
namespace parser {
namespace expression {

/**
 * Represents a unary operator, e.g. unary minus.
 */
class OperatorUnaryExpression : public AbstractExpression {
  explicit OperatorUnaryExpression(const ExpressionType expression_type, const TypeId return_value_type,
                                   AbstractExpression *operand)
      : AbstractExpression(expression_type, return_value_type, operand) {}

  AbstractExpression *Copy() const override { return new OperatorUnaryExpression(*this); }
};

/**
 * Represents a binary operator, e.g. plus, minus, times.
 */
class OperatorBinaryExpression : public AbstractExpression {
  explicit OperatorBinaryExpression(const ExpressionType expression_type, const TypeId return_value_type,
                                    AbstractExpression *left, AbstractExpression *right)
      : AbstractExpression(expression_type, return_value_type, left, right) {}

  AbstractExpression *Copy() const override { return new OperatorBinaryExpression(*this); }
};

}  // namespace expression
}  // namespace parser
}  // namespace terrier
