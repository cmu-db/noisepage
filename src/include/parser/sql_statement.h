#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "binder/sql_node_visitor.h"
#include "catalog/catalog_defs.h"
#include "common/error/exception.h"
#include "common/hash_util.h"
#include "common/json_header.h"
#include "common/macros.h"
#include "parser/expression/abstract_expression.h"
#include "parser/parser_defs.h"

namespace noisepage {

namespace binder {
class BindNodeVisitor;
}  // namespace binder

namespace parser {
class ParseResult;

class AbstractExpression;

/**
 * Table location information (Database, Namespace, Table).
 */
struct TableInfo {
  /**
   * @param table_name table name
   * @param namespace_name namespace name
   * @param database_name database name
   */
  TableInfo(std::string table_name, std::string namespace_name, std::string database_name)
      : table_name_(std::move(table_name)),
        namespace_name_(std::move(namespace_name)),
        database_name_(std::move(database_name)) {}

  TableInfo() = default;

  /**
   * @return a copy of the table location information
   */
  std::unique_ptr<TableInfo> Copy() {
    return std::make_unique<TableInfo>(GetTableName(), GetNamespaceName(), GetDatabaseName());
  }

  /**
   * @return table name
   */
  const std::string &GetTableName() { return table_name_; }

  /**
   * @return namespace name
   */
  const std::string &GetNamespaceName() { return namespace_name_; }

  /**
   * @return database name
   */
  const std::string &GetDatabaseName() { return database_name_; }

  /**
   * @return the hashed value of this table info object
   */
  common::hash_t Hash() const {
    common::hash_t hash = common::HashUtil::Hash(table_name_);
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(namespace_name_));
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(database_name_));
    return hash;
  }

  /**
   * Logical equality check.
   * @param rhs other
   * @return true if the two TableInfo are logically equal
   */
  bool operator==(const TableInfo &rhs) const {
    if (table_name_ != rhs.table_name_) return false;
    if (namespace_name_ != rhs.namespace_name_) return false;
    return database_name_ == rhs.database_name_;
  }

  /**
   * Logical inequality check.
   * @param rhs other
   * @return true if the two TableInfo logically unequal
   */
  bool operator!=(const TableInfo &rhs) const { return !(operator==(rhs)); }

  /**
   * @return TableInfo serialized to json
   */
  nlohmann::json ToJson() const;

  /**
   * @param j json to deserialize
   */
  std::vector<std::unique_ptr<AbstractExpression>> FromJson(const nlohmann::json &j);

 private:
  friend class TableRefStatement;
  friend class TableRef;
  std::string table_name_;
  std::string namespace_name_;
  std::string database_name_;
};

DEFINE_JSON_HEADER_DECLARATIONS(TableInfo);

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

  /**
   * Default constructor for deserialization
   */
  SQLStatement() = default;

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
   * @param v Visitor pattern for the statement
   */
  virtual void Accept(common::ManagedPointer<binder::SqlNodeVisitor> v) = 0;

  /**
   * @return statement serialized to json
   */
  virtual nlohmann::json ToJson() const;

  /**
   * @param j json to deserialize
   */
  virtual std::vector<std::unique_ptr<parser::AbstractExpression>> FromJson(const nlohmann::json &j);

 private:
  StatementType stmt_type_;
};

DEFINE_JSON_HEADER_DECLARATIONS(SQLStatement);

/**
 * Base class for statements that refer to other tables.
 */
class TableRefStatement : public SQLStatement {
 public:
  /**
   * @param type type of SQLStatement being referred to
   * @param table_info table being referred to
   */
  TableRefStatement(const StatementType type, std::unique_ptr<TableInfo> table_info)
      : SQLStatement(type), table_info_(std::move(table_info)) {
    if (!table_info_) table_info_ = std::make_unique<TableInfo>();
  }

  ~TableRefStatement() override = default;

  /**
   * @return table name
   */
  virtual const std::string &GetTableName() const { return table_info_->GetTableName(); }

  /**
   * @return namespace name
   */
  virtual const std::string &GetNamespaceName() const { return table_info_->GetNamespaceName(); }

  /**
   * @return database name
   */
  virtual const std::string &GetDatabaseName() const { return table_info_->GetDatabaseName(); }

 private:
  friend class binder::BindNodeVisitor;
  std::unique_ptr<TableInfo> table_info_ = nullptr;
};

}  // namespace parser
}  // namespace noisepage
