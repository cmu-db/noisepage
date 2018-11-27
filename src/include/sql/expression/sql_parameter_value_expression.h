#pragma once
#include <memory>
#include <vector>
#include "sql/expression/sql_abstract_expression.h"
#include "type/type_id.h"

namespace terrier::sql {

/**
 * Represents a parameter's offset in an expression.
 */
class SqlParameterValueExpression : public SqlAbstractExpression {
 public:
  std::unique_ptr<SqlAbstractExpression> Copy() const override {
    return std::make_unique<SqlParameterValueExpression>(*this);
  }

  /**
   * @return offset in the expression
   */
  uint32_t GetValueIdx() { return value_idx_; }

  /**
   * Builder for building a SqlParameterValueExpression
   */
  class Builder : public SqlAbstractExpression::Builder<Builder> {
   public:
    Builder &SetValueIdx(uint32_t value_idx) {
      value_idx_ = value_idx;
      return *this;
    }

    std::shared_ptr<SqlParameterValueExpression> Build() {
      return std::shared_ptr<SqlParameterValueExpression>(new SqlParameterValueExpression(value_idx_));
    }

   private:
    uint32_t value_idx_;
  };
  friend class Builder;

 private:
  // TODO(Tianyu): Can we get a better name for this?
  const uint32_t value_idx_;

  /**
   * Instantiates a new SqlParameterValueExpression with the given offset.
   * @param value_idx the offset of the parameter
   */
  explicit SqlParameterValueExpression(const uint32_t value_idx)
      : SqlAbstractExpression(parser::ExpressionType::VALUE_PARAMETER, type::TypeId::PARAMETER_OFFSET, {}),
        value_idx_(value_idx) {}
};

}  // namespace terrier::sql
