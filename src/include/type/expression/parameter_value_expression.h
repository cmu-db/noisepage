#pragma once

#include "type/expression/abstract_expression.h"
#include "type/type_id.h"

namespace terrier {
namespace type {
namespace expression {

/**
 * Represents a parameter's offset in an expression.
 */
class ParameterValueExpression : public AbstractExpression {
 public:
  /**
   * Instantiates a new ParameterValueExpression with the given offset.
   * @param value_idx the offset of the parameter
   */
  explicit ParameterValueExpression(const uint32_t value_idx)
      : AbstractExpression(ExpressionType::VALUE_PARAMETER, TypeId::PARAMETER_OFFSET), value_idx_(value_idx) {}

  AbstractExpression *Copy() const override { return new ParameterValueExpression(value_idx_); }

 private:
  uint32_t value_idx_;
};

}  // namespace expression
}  // namespace type
}  // namespace terrier
