#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "sql/expression/sql_abstract_expression.h"
#include "parser/expression_defs.h"
#include "type/type_id.h"

namespace terrier::sql {

/**
 * Represents a logical tuple value.
 */
class SqlTupleValueExpression : public SqlAbstractExpression {
 public:
  /**
   * Creates a tuple value expression with the given column and table name.
   *
   */
  SqlTupleValueExpression(std::string col_name, std::string table_name)
      : SqlAbstractExpression(parser::ExpressionType::VALUE_TUPLE, type::TypeId::INVALID, {}),
        col_name_(std::move(col_name)),
        table_name_(std::move(table_name)) {}

  std::unique_ptr<SqlAbstractExpression> Copy() const override { return std::make_unique<SqlTupleValueExpression>(*this); }

 private:
  const std::string col_name_;
  const std::string table_name_;
};

}  // namespace terrier::sql
