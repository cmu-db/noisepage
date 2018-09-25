#pragma once

#include <vector>
#include "type/expression/abstract_expression.h"
#include "type/expression/expression_defs.h"
#include "type/type_id.h"

namespace terrier {
namespace type {
namespace expression {

/**
 * Represents a logical comparison expression.
 */
class ComparisonExpression : public AbstractExpression {
 public:
  /**
   * Instantiates a new comparison expression.
   * @param cmp_type type of comparison
   * @param left left operand
   * @param right right operand
   */
  explicit ComparisonExpression(const ExpressionType cmp_type, AbstractExpression *left, AbstractExpression *right)
      : AbstractExpression(cmp_type, TypeId::BOOLEAN, left, right) {}

  AbstractExpression *Copy() const override {
    return new ComparisonExpression(GetExpressionType(), GetChild(0), GetChild(1));
  }
};

}  // namespace expression
}  // namespace type
}  // namespace terrier
