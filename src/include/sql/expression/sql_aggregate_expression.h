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
  /**
   * Instantiates a new aggregate expression.
   * @param type type of aggregate expression
   * @param children children to be added
   */
  SqlAggregateExpression(parser::ExpressionType type, std::vector<std::shared_ptr<SqlAbstractExpression>> &&children)
      : SqlAbstractExpression(type, type::TypeId::INVALID, std::move(children)) {}

  std::unique_ptr<SqlAbstractExpression> Copy() const override {
    return std::make_unique<SqlAggregateExpression>(*this);
  }

  class Builder : public SqlAbstractExpression::Builder<Builder> {
   public:
    std::shared_ptr<SqlAggregateExpression> Build() {
      return std::shared_ptr<SqlAggregateExpression>(
          new SqlAggregateExpression(expression_type_, std::move(children_));
    }
  };
};

}  // namespace terrier::sql
