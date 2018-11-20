#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "parser/expression_defs.h"
#include "sql/expression/sql_abstract_expression.h"
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

  std::unique_ptr<SqlAbstractExpression> Copy() const override {
    return std::make_unique<SqlTupleValueExpression>(*this);
  }

  class Builder : public SqlAbstractExpression::Builder<Builder> {
   public:
    Builder &SetColName(std::string col_name) {
      col_name_ = col_name;
      return *this;
    }

    Builder &SetTableName(sstd::string table_name) {
      table_name_ = table_name;
      return *this;
    }

    std::shared_ptr<SqlTupleValueExpression> Build() {
      return std::shared_ptr<SqlTupleValueExpression>(new SqlTupleValueExpression(col_name_, table_name_);
    }

   private:
    const std::string col_name_;
    const std::string table_name_;
  };
  friend class Builder;

 private:
  const std::string col_name_;
  const std::string table_name_;
};

}  // namespace terrier::sql
