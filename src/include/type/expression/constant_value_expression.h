#pragma once

#include "common/hash_util.h"
#include "type/expression/abstract_expression.h"
#include "type/value.h"

namespace terrier {
namespace type {
namespace expression {

/**
 * Represents a logical constant expression.
 */
class ConstantValueExpression : public AbstractExpression {
 public:
  /**
   * Instantiate a new constant value expression.
   * @param value value to be held
   */
  explicit ConstantValueExpression(const type::Value &value)
      : AbstractExpression(ExpressionType::VALUE_CONSTANT, value.type_id), value_(value) {}

  hash_t Hash() const override {
    ExpressionType expr = GetExpressionType();
    return HashUtil::CombineHashes(HashUtil::Hash(&expr), HashUtil::Hash(value_.contents));
  }

  bool operator==(const AbstractExpression &other) const override {
    if (GetExpressionType() != other.GetExpressionType()) {
      return false;
    }
    auto const &const_expr = dynamic_cast<const ConstantValueExpression &>(other);
    return value_.contents == const_expr.GetValue().contents;
  }

  bool operator!=(const AbstractExpression &rhs) const override { return !(*this == rhs); }

  AbstractExpression *Copy() const override { return new ConstantValueExpression(GetValue()); }

  /**
   * Returns the constant value stored in this constant value expression.
   * @return the value of the constant
   */
  type::Value GetValue() const { return value_; }

 private:
  type::Value value_;
};

}  // namespace expression
}  // namespace type
}  // namespace terrier
