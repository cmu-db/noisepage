#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "catalog/catalog_defs.h"
#include "common/macros.h"
#include "common/sql_node_visitor.h"
#include "parser/parser_defs.h"

namespace terrier {
namespace parser {

/**
 * Table location information (Database, Schema, Table).
 */
struct TableInfo {
  /**
   * @param table_name table name
   * @param schema_name schema name
   * @param database_name database name
   */
  TableInfo(std::string table_name, std::string schema_name, std::string database_name)
      : table_name_(std::move(table_name)),
        schema_name_(std::move(schema_name)),
        database_name_(std::move(database_name)) {}

  /**
   * @return table name
   */
  std::string GetTableName() { return table_name_; }

  /**
   * @return schema name
   */
  std::string GetSchemaName() { return schema_name_; }

  /**
   * @return database name
   */
  std::string GetDatabaseName() { return database_name_; }

 private:
  const std::string table_name_;
  const std::string schema_name_;
  const std::string database_name_;
};

/**
 * Base class for the parsed SQL statements.
 */
class SQLStatement {
 public:
  /**
   * Create a new SQL statement.
   * @param type SQL statement type
   */
  explicit SQLStatement(StatementType type) : stmt_type_(type) {}

  virtual ~SQLStatement() = default;

  /**
   * @return SQL statement type
   */
  virtual StatementType GetType() const { return stmt_type_; }

  // TODO(WAN): do we need this? how is it used?
  // Visitor Pattern used for the optimizer to access statements
  // This allows a facility outside the object itself to determine the type of
  // class using the built-in type system.
  /**
   * Visitor pattern used to access and create optimizer objects.
   * TODO(WAN): this probably can be better described by WEN
   * @param v visitor
   */
  virtual void Accept(SqlNodeVisitor *v) = 0;

 private:
  StatementType stmt_type_;
};

/**
 * Base class for statements that refer to other tables.
 */
class TableRefStatement : public SQLStatement {
 public:
  /**
   * @param type type of SQLStatement being referred to
   * @param table_info table being referred to
   */
  TableRefStatement(const StatementType type, std::shared_ptr<TableInfo> table_info)
      : SQLStatement(type), table_info_(std::move(table_info)) {}

  ~TableRefStatement() override = default;

  /**
   * @return table name
   */
  virtual inline std::string GetTableName() const { return table_info_->GetTableName(); }

  /**
   * @return table schema name (aka namespace)
   */
  virtual inline std::string GetSchemaName() const { return table_info_->GetSchemaName(); }

  /**
   * @return database name
   */
  virtual inline std::string GetDatabaseName() const { return table_info_->GetDatabaseName(); }

 private:
  const std::shared_ptr<TableInfo> table_info_ = nullptr;
};

}  // namespace parser
}  // namespace terrier
