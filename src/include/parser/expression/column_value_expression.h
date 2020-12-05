#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "catalog/catalog_defs.h"
#include "parser/expression/abstract_expression.h"

namespace noisepage {
class TpccPlanTest;
}  // namespace noisepage

namespace noisepage::optimizer {
class OptimizerUtil;
}  // namespace noisepage::optimizer

namespace noisepage::binder {
class BinderContext;
}

namespace noisepage::execution::sql {
class TableGenerator;
}  // namespace noisepage::execution::sql

namespace noisepage::parser {

/**
 * ColumnValueExpression represents a reference to a column.
 */
class ColumnValueExpression : public AbstractExpression {
  // PlanGenerator creates ColumnValueexpressions and will
  // need to set the bound oids
  friend class noisepage::optimizer::OptimizerUtil;
  friend class noisepage::TpccPlanTest;

 public:
  /**
   * This constructor is called only in postgresparser, setting the column name,
   * and optionally setting the table name and alias.
   * @param table_alias table name
   * @param col_name column name
   * @param alias alias of the expression
   */
  ColumnValueExpression(AliasType table_alias, std::string col_name, AliasType alias)
      : AbstractExpression(ExpressionType::COLUMN_VALUE, type::TypeId::INVALID, std::move(alias), {}),
        table_alias_(std::move(table_alias)),
        column_name_(std::move(col_name)) {}

  /**
   * @param table_alias table name
   * @param col_name column name
   */
  ColumnValueExpression(AliasType table_alias, std::string col_name)
      : AbstractExpression(ExpressionType::COLUMN_VALUE, type::TypeId::INVALID, {}),
        table_alias_(std::move(table_alias)),
        column_name_(std::move(col_name)) {}

  /**
   * @param database_oid database OID
   * @param table_oid table OID
   * @param column_oid column OID
   */
  ColumnValueExpression(catalog::db_oid_t database_oid, catalog::table_oid_t table_oid, catalog::col_oid_t column_oid)
      : AbstractExpression(ExpressionType::COLUMN_VALUE, type::TypeId::INVALID, {}),
        database_oid_(database_oid),
        table_oid_(table_oid),
        column_oid_(column_oid) {}

  /**
   * @param table_oid OID of the table.
   * @param column_oid OID of the column.
   * @param type Type of the column.
   */
  ColumnValueExpression(catalog::table_oid_t table_oid, catalog::col_oid_t column_oid, type::TypeId type)
      : AbstractExpression(ExpressionType::COLUMN_VALUE, type, {}), table_oid_(table_oid), column_oid_(column_oid) {}

  /**
   * This constructor is used to construct abstract value expressions used by CTEs
   * for LogicalQueryDerivedGet's below it to reference aliases.
   * @param table_alias Name of the table
   * @param col_name name of the column.
   * @param type Type of the column.
   * @param alias Alias of the column this is referencing
   * @param column_oid Oid of the column (it should be a temp oid in this case)
   */
  ColumnValueExpression(AliasType table_alias, std::string col_name, type::TypeId type, AliasType alias,
                        catalog::col_oid_t column_oid)
      : AbstractExpression(ExpressionType::COLUMN_VALUE, type, std::move(alias), {}),
        table_alias_(std::move(table_alias)),
        column_name_(std::move(col_name)),
        column_oid_(column_oid) {}

  /**
   * @param table_alias table name
   * @param col_name column name
   * @param database_oid database OID
   * @param table_oid table OID
   * @param column_oid column OID
   * @param type Type of the column.
   */
  ColumnValueExpression(AliasType table_alias, std::string col_name, catalog::db_oid_t database_oid,
                        catalog::table_oid_t table_oid, catalog::col_oid_t column_oid, type::TypeId type)
      : AbstractExpression(ExpressionType::COLUMN_VALUE, type, {}),
        table_alias_(std::move(table_alias)),
        column_name_(std::move(col_name)),
        database_oid_(database_oid),
        table_oid_(table_oid),
        column_oid_(column_oid) {}

  /** Default constructor for deserialization. */
  ColumnValueExpression() = default;

  /** @return table name */
  AliasType GetTableAlias() const { return table_alias_; }

  /** @param table_oid Table OID to be assigned to this expression */
  void SetTableAlias(const AliasType &table_alias) { table_alias_ = AliasType(table_alias); }

  /** @return column name */
  std::string GetColumnName() const { return column_name_; }

  /** @return database oid */
  catalog::db_oid_t GetDatabaseOid() const { return database_oid_; }

  /** @return table oid */
  catalog::table_oid_t GetTableOid() const { return table_oid_; }

  /** @return column oid */
  catalog::col_oid_t GetColumnOid() const { return column_oid_; }

  /**
   * Get Column Full Name [tbl].[col]
   */
  std::string GetFullName() const {
    if (!table_alias_.Empty()) {
      return table_alias_.GetName() + "." + column_name_;
    }

    return column_name_;
  }

  /**
   * Copies this ColumnValueExpression
   * @returns copy of this
   */
  std::unique_ptr<AbstractExpression> Copy() const override;

  /**
   * Copies this ColumnValueExpression with new children
   * @param children new children
   * @returns copy of this with new children
   */
  std::unique_ptr<AbstractExpression> CopyWithChildren(
      std::vector<std::unique_ptr<AbstractExpression>> &&children) const override {
    NOISEPAGE_ASSERT(children.empty(), "ColumnValueExpression should have no children");
    return Copy();
  }

  /**
   * Hashes the current ColumnValue expression.
   */
  common::hash_t Hash() const override;

  /**
   * Logical equality check.
   * @param rhs other
   * @return true if the two expressions are logically equal
   */
  bool operator==(const AbstractExpression &rhs) const override;

  /**
   * Walks the expression trees and generate the correct expression name
   */
  void DeriveExpressionName() override;

  void Accept(common::ManagedPointer<binder::SqlNodeVisitor> v) override;

  /**
   * @return expression serialized to json
   */
  nlohmann::json ToJson() const override;

  /**
   * @param j json to deserialize
   */
  std::vector<std::unique_ptr<AbstractExpression>> FromJson(const nlohmann::json &j) override;

 private:
  friend class binder::BinderContext;
  friend class execution::sql::TableGenerator;
  /** @param database_oid Database OID to be assigned to this expression */
  void SetDatabaseOID(catalog::db_oid_t database_oid) { database_oid_ = database_oid; }
  /** @param table_oid Table OID to be assigned to this expression */
  void SetTableOID(catalog::table_oid_t table_oid) { table_oid_ = table_oid; }
  /** @param column_oid Column OID to be assigned to this expression */
  void SetColumnOID(catalog::col_oid_t column_oid) { column_oid_ = column_oid; }
  /** @param column_oid Column OID to be assigned to this expression */
  void SetColumnName(const std::string &col_name) { column_name_ = std::string(col_name); }

  /** Table name. */
  AliasType table_alias_;
  /** Column name. */
  std::string column_name_;

  /** OID of the database */
  catalog::db_oid_t database_oid_ = catalog::INVALID_DATABASE_OID;

  /** OID of the table */
  catalog::table_oid_t table_oid_ = catalog::INVALID_TABLE_OID;

  /** OID of the column */
  catalog::col_oid_t column_oid_ = catalog::INVALID_COLUMN_OID;
};

DEFINE_JSON_HEADER_DECLARATIONS(ColumnValueExpression);

}  // namespace noisepage::parser
