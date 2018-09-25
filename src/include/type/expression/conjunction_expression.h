#pragma once

#include "type/expression/abstract_expression.h"
#include "type/expression/expression_defs.h"
#include "type/type_id.h"

namespace terrier {
namespace type {
namespace expression {

/**
 * Represents a logical conjunction expression.
 */
class ConjunctionExpression : public AbstractExpression {
 public:
  /**
   * Instantiates a new conjunction expression.
   * @param cmp_type type of conjunction
   * @param left left operand
   * @param right right operand
   */
  explicit ConjunctionExpression(const ExpressionType cmp_type, AbstractExpression *left, AbstractExpression *right)
      : AbstractExpression(cmp_type, TypeId::BOOLEAN, left, right) {}

  AbstractExpression *Copy() const override {
    return new ConjunctionExpression(GetExpressionType(), GetChild(0), GetChild(1));
  }
};

}  // namespace expression
}  // namespace type
}  // namespace terrier
