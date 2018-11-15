#pragma once

#include <memory>
#include <utility>
#include <vector>
#include "sql/expression/sql_abstract_expression.h"
#include "parser/expression_defs.h"
#include "type/type_id.h"

namespace terrier::sql {

/**
 * Represents a logical comparison expression.
 */
class SqlComparisonExpression : public SqlAbstractExpression {
 public:
  /**
   * Instantiates a new comparison expression.
   * @param cmp_type type of comparison
   * @param children vector containing exactly two children, left then right
   */
  SqlComparisonExpression(const parser::ExpressionType cmp_type, std::vector<std::shared_ptr<SqlAbstractExpression>> &&children)
      : SqlAbstractExpression(cmp_type, type::TypeId::BOOLEAN, std::move(children)) {}

  std::unique_ptr<SqlAbstractExpression> Copy() const override { return std::make_unique<SqlComparisonExpression>(*this); }

  class Builder : public SqlAbstractExpression::Builder<Builder> {
   public:
    std::shared_ptr<SqlComparisonExpression> Build() {
      return std::shared_ptr<SqlComparisonExpression>(
          new SqlComparisonExpression(expression_type_, std::move(children_));
    }
  };
};
}  // namespace terrier::sql
