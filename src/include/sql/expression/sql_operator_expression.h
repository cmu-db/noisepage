#pragma once

#include <memory>
#include <utility>
#include <vector>
#include "sql/expression/sql_abstract_expression.h"
#include "parser/expression_defs.h"
#include "type/value.h"

namespace terrier::sql {

/**
 * Represents an operator.
 */
class SqlOperatorExpression : public SqlAbstractExpression {
  /**
   * Instantiates a new unary operator.
   * @param expression_type type of operator
   * @param return_value_type return type of the operator
   * @param children vector containing arguments to the operator left to right
   */
  SqlOperatorExpression(const parser::ExpressionType expression_type, const type::TypeId return_value_type,
                        std::vector<std::shared_ptr<SqlAbstractExpression>> &&children)
      : SqlAbstractExpression(expression_type, return_value_type, std::move(children)) {}

  std::unique_ptr<SqlAbstractExpression> Copy() const override { return std::make_unique<SqlOperatorExpression>(*this); }

  class Builder : public SqlAbstractExpression::Builder<Builder> {
   public:
    std::shared_ptr<SqlOperatorExpression> Build() {
      return std::shared_ptr<SqlOperatorExpression>(
          new SqlOperatorExpression(expression_type_, return_value_type_, std::move(children_));
    }
  };

};

}  // namespace terrier::sql
