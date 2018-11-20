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

struct TableInfo {
  TableInfo(std::string table_name, std::string schema_name, std::string database_name)
      : table_name_(std::move(table_name)),
        schema_name_(std::move(schema_name)),
        database_name_(std::move(database_name)) {}
  // member variables
  const std::string table_name_;
  const std::string schema_name_;
  const std::string database_name_;
};

/**
 * Base class for the parsed SQL statements.
 */
class SQLStatement {
 public:
  explicit SQLStatement(StatementType type) : stmt_type_(type) {}

  virtual ~SQLStatement() = default;

  virtual StatementType GetType() const { return stmt_type_; }

  // TODO(WAN): do we need this? how is it used?
  // Visitor Pattern used for the optimizer to access statements
  // This allows a facility outside the object itself to determine the type of
  // class using the built-in type system.
  virtual void Accept(SqlNodeVisitor *v) = 0;

 private:
  StatementType stmt_type_;
};

/**
 * Base class for statements that refer to other tables.
 */
class TableRefStatement : public SQLStatement {
 public:
  TableRefStatement(const StatementType type, std::unique_ptr<TableInfo> table_info)
      : SQLStatement(type), table_info_(std::move(table_info)) {}

  ~TableRefStatement() override = default;
  /*
    virtual inline void TryBindDatabaseName(const std::string &default_database_name) {
      if (table_info_ == nullptr) {
        table_info_ = std::make_unique<TableInfo>()
      }

      if (!table_info_) table_info_.reset(new parser::TableInfo());

      if (table_info_->database_name_.empty()) table_info_->database_name = default_database_name;
      // if schema name is not specified, then it's default value is "public"
      if (table_info_->schema_name_.empty()) table_info_->schema_name = catalog::DEFAULT_SCHEMA_NAME;
    }
  */
  virtual inline std::string GetTableName() const { return table_info_->table_name_; }

  // Get the name of the schema(namespace) of this table
  virtual inline std::string GetSchemaName() const { return table_info_->schema_name_; }

  // Get the name of the database of this table
  virtual inline std::string GetDatabaseName() const { return table_info_->database_name_; }

 private:
  const std::unique_ptr<TableInfo> table_info_ = nullptr;
};

}  // namespace parser
}  // namespace terrier
