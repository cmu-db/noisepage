#pragma once

#include <memory>
#include <vector>
#include "common/hash_util.h"
#include "sql/expression/sql_abstract_expression.h"
#include "type/value.h"

namespace terrier::sql {

/**
 * Represents a logical constant expression.
 */
class SqlConstantValueExpression : public SqlAbstractExpression {
 public:
  common::hash_t Hash() const override {
    return common::HashUtil::CombineHashes(SqlAbstractExpression::Hash(), value_.Hash());
  }

  bool operator==(const SqlAbstractExpression &other) const override {
    if (GetExpressionType() != other.GetExpressionType()) {
      return false;
    }
    auto const &const_expr = dynamic_cast<const SqlConstantValueExpression &>(other);
    return value_ == const_expr.GetValue();
  }

  std::unique_ptr<SqlAbstractExpression> Copy() const override {
    return std::make_unique<SqlConstantValueExpression>(*this);
  }

  /**
   * @return the constant value stored in this expression
   */
  type::Value GetValue() const { return value_; }

  /**
   * Builder for building a SqlConstantValueExpression
   */
  class Builder : public SqlAbstractExpression::Builder<Builder> {
   public:
    Builder &SetValue(type::Value value) {
      value_ = value;
      return *this;
    }

    std::shared_ptr<SqlConstantValueExpression> Build() {
      return std::shared_ptr<SqlConstantValueExpression>(new SqlConstantValueExpression(value_));
    }

   private:
    type::Value value_;
  };
  friend class Builder;

 private:
  type::Value value_;

  /**
   * Instantiate a new constant value expression.
   * @param value value to be held
   */
  explicit SqlConstantValueExpression(const type::Value &value)
      : SqlAbstractExpression(parser::ExpressionType::VALUE_CONSTANT, value.GetType(), {}), value_(value) {}
};

}  // namespace terrier::sql
