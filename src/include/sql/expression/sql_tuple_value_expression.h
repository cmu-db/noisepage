#pragma once

#include <memory>
#include <tuple>
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
  SqlTupleValueExpression(std::tuple<table_oid_t, col_oid_t, tuple_oid_t> obj_id, std::string col_name,
                          std::string table_name)
      : SqlAbstractExpression(parser::ExpressionType::VALUE_TUPLE, type::TypeId::INVALID, {}),
        obj_id_(std::move(obj_id)),
        col_name_(std::move(col_name)),
        table_name_(std::move(table_name)) {}

  std::unique_ptr<SqlAbstractExpression> Copy() const override {
    return std::make_unique<SqlTupleValueExpression>(*this);
  }

  class Builder : public SqlAbstractExpression::Builder<Builder> {
   public:
    Builder &SetOID(std::tuple<table_oid_t, col_oid_t, tuple_oid_t> obj_id) {
      obj_id_ = obj_id;
      return *this;
    }

    Builder &SetColName(std::string col_name) {
      col_name_ = col_name;
      return *this;
    }

    Builder &SetTableName(std::string table_name) {
      table_name_ = table_name;
      return *this;
    }

    std::shared_ptr<SqlTupleValueExpression> Build() {
      return std::shared_ptr<SqlTupleValueExpression>(new SqlTupleValueExpression(obj_id_, col_name_, table_name_);
    }

   private:
    std::tuple<table_oid_t, col_oid_t, tuple_oid_t> obj_id_;
    std::string col_name_;
    std::string table_name_;
  };
  friend class Builder;

 private:
  const std::tuple<table_oid_t, col_oid_t, tuple_oid_t> obj_id_;
  const std::string col_name_;
  const std::string table_name_;
};

}  // namespace terrier::sql
