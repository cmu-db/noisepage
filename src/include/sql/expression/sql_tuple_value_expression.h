#pragma once

#include <memory>
#include <string>
#include <tuple>
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
  std::unique_ptr<SqlAbstractExpression> Copy() const override {
    return std::make_unique<SqlTupleValueExpression>(*this);
  }

  bool operator==(const SqlAbstractExpression &rhs) const override {
    auto &other = static_cast<const SqlTupleValueExpression &>(rhs);
    // For query like SELECT A.id, B.id FROM test AS A, test AS B;
    // we need to know whether A.id is from A.id or B.id. In this case,
    // A.id and B.id have the same bound oids since they refer to the same table
    // but they have different table alias.
    if ((table_name_.empty() != other.table_name_.empty()) || col_name_.empty() != other.col_name_.empty())
      return false;
    bool res = obj_id_ == other.obj_id_;
    if (!table_name_.empty() && !other.table_name_.empty()) res = table_name_ == other.table_name_ && res;
    if (!col_name_.empty() && !other.col_name_.empty()) res = col_name_ == other.col_name_ && res;
    return res;
  }

  std::string GetTableName() const { return table_name_; }

  std::string GetColumnName() const { return col_name_; }

  std::string GetColFullName() const {
    if (!table_name_.empty()) {
      return table_name_ + "." + col_name_;
    }
    return col_name_;
  }

  const std::tuple<db_oid_t, table_oid_t, col_oid_t> &GetOid() const { return obj_id_; }

  /**
   * Builder for building a SqlTupleValueExpression
   */
  class Builder : public SqlAbstractExpression::Builder<Builder> {
   public:
    Builder &SetOid(std::tuple<db_oid_t, table_oid_t, col_oid_t> obj_id) {
      obj_id_ = obj_id;
      return *this;
    }

    Builder &SetColName(std::string col_name) {
      col_name_ = std::move(col_name);
      return *this;
    }

    Builder &SetTableName(std::string table_name) {
      table_name_ = std::move(table_name);
      return *this;
    }

    std::shared_ptr<SqlTupleValueExpression> Build() {
      return std::shared_ptr<SqlTupleValueExpression>(new SqlTupleValueExpression(obj_id_, col_name_, table_name_));
    }

   private:
    std::tuple<db_oid_t, table_oid_t, col_oid_t> obj_id_;
    std::string col_name_;
    std::string table_name_;
  };
  friend class Builder;

 private:
  const std::tuple<db_oid_t, table_oid_t, col_oid_t> obj_id_;
  const std::string col_name_;
  const std::string table_name_;

  /**
   * Creates a tuple value expression with the given column and table name.
   *
   */
  SqlTupleValueExpression(std::tuple<db_oid_t, table_oid_t, col_oid_t> obj_id, std::string col_name,
                          std::string table_name)
      : SqlAbstractExpression(parser::ExpressionType::VALUE_TUPLE, type::TypeId::INVALID, {}),
        obj_id_(std::move(obj_id)),
        col_name_(std::move(col_name)),
        table_name_(std::move(table_name)) {}
};

}  // namespace terrier::sql
