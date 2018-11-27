#pragma once

#include <memory>
#include <utility>
#include <vector>
#include "parser/expression_defs.h"
#include "sql/expression/sql_abstract_expression.h"
#include "type/type_id.h"

namespace terrier::sql {

/**
 * An SqlAggregateExpression is only used for parsing, planning and optimizing.
 */
class SqlAggregateExpression : public SqlAbstractExpression {
 public:
  std::unique_ptr<SqlAbstractExpression> Copy() const override {
    return std::make_unique<SqlAggregateExpression>(*this);
  }

  /**
   * @return value of this expression
   */
  const int GetValue() const { return value_idx_; }

  /**
   * @return column OID of this expression
   */
  const col_oid_t GetColOid() const { return col_oid_; }

  /**
   * Builder for building a SqlAggregateExpression
   */
  class Builder : public SqlAbstractExpression::Builder<Builder> {
   public:
    Builder &SetValue(int value_idx) {
      value_idx_ = value_idx;
      return *this;
    }

    Builder &SetColOid(col_oid_t col_oid) {
      col_oid_ = col_oid;
      return *this;
    }

    std::shared_ptr<SqlAggregateExpression> Build() {
      return std::shared_ptr<SqlAggregateExpression>(
          new SqlAggregateExpression(value_idx_, col_oid_, std::move(expression_type_), std::move(children_)));
    }

   private:
    int value_idx_;
    col_oid_t col_oid_;
  };
  friend class Builder;

 private:
  const int value_idx_;
  const col_oid_t col_oid_;

  /**
   * Instantiates a new aggregate expression.
   * @param type type of aggregate expression
   * @param children children to be added
   */
  SqlAggregateExpression(int value_idx, col_oid_t col_oid, parser::ExpressionType type,
                         std::vector<std::shared_ptr<SqlAbstractExpression>> &&children)
      : SqlAbstractExpression(type, type::TypeId::INVALID, std::move(children)),
        value_idx_(std::move(value_idx)),
        col_oid_(std::move(col_oid)) {}
};

}  // namespace terrier::sql
