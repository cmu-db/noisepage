#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "binder/sql_node_visitor.h"
#include "catalog/catalog_defs.h"
#include "common/exception.h"
#include "common/hash_util.h"
#include "common/json.h"
#include "common/macros.h"
#include "loggers/parser_logger.h"
#include "parser/expression/abstract_expression.h"
#include "parser/parser_defs.h"
#include "type/type_id.h"

namespace terrier {

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
  nlohmann::json ToJson() const {
    nlohmann::json j;
    j["table_name"] = table_name_;
    j["namespace_name"] = namespace_name_;
    j["database_name"] = database_name_;
    return j;
  }

  /**
   * @param j json to deserialize
   */
  std::vector<std::unique_ptr<AbstractExpression>> FromJson(const nlohmann::json &j) {
    std::vector<std::unique_ptr<AbstractExpression>> exprs;
    table_name_ = j.at("table_name").get<std::string>();
    namespace_name_ = j.at("namespace_name").get<std::string>();
    database_name_ = j.at("database_name").get<std::string>();
    return exprs;
  }

 private:
  friend class TableRefStatement;
  friend class TableRef;
  std::string table_name_;
  std::string namespace_name_;
  std::string database_name_;
};

DEFINE_JSON_DECLARATIONS(TableInfo);

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
  virtual nlohmann::json ToJson() const {
    nlohmann::json j;
    j["stmt_type"] = stmt_type_;
    return j;
  }

  /**
   * @param j json to deserialize
   */
  virtual std::vector<std::unique_ptr<parser::AbstractExpression>> FromJson(const nlohmann::json &j) {
    std::vector<std::unique_ptr<parser::AbstractExpression>> exprs;
    stmt_type_ = j.at("stmt_type").get<StatementType>();
    return exprs;
  }

 private:
  StatementType stmt_type_;
};

DEFINE_JSON_DECLARATIONS(SQLStatement);

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

/**
 * ColumnDefinition represents the logical description of a table column.
 */
struct ColumnDefinition {
  // TODO(WAN): I really hate how everything is mashed together.
  // There were also a number of unused attributes e.g. primary_keys, multi_unique...
  // that were never used.

  /**
   * Column data types.
   */
  enum class DataType {
    INVALID,

    PRIMARY,
    FOREIGN,
    MULTIUNIQUE,

    CHAR,
    INT,
    INTEGER,
    TINYINT,
    SMALLINT,
    BIGINT,
    DOUBLE,
    FLOAT,
    DECIMAL,
    BOOLEAN,
    ADDRESS,
    DATE,
    TIMESTAMP,
    TEXT,

    VARCHAR,
    VARBINARY
  };

  /**
   * Foreign key constructor.
   * @param fk_sources foreign key sources
   * @param fk_sinks foreign key sinks
   * @param fk_sink_table_name foreign key sink table name
   * @param delete_action action to take upon deletion
   * @param update_action action to take upon update
   * @param match_type type of foreign key match
   */
  ColumnDefinition(std::vector<std::string> fk_sources, std::vector<std::string> fk_sinks,
                   std::string fk_sink_table_name, FKConstrActionType delete_action, FKConstrActionType update_action,
                   FKConstrMatchType match_type)
      : type_(DataType::FOREIGN),
        fk_sources_(std::move(fk_sources)),
        fk_sinks_(std::move(fk_sinks)),
        fk_sink_table_name_(std::move(fk_sink_table_name)),
        fk_delete_action_(delete_action),
        fk_update_action_(update_action),
        fk_match_type_(match_type) {}

  /**
   * Table column constructor.
   * @param name column name
   * @param type column type
   * @param is_primary is primary key
   * @param is_not_null is not nullable
   * @param is_unique is unique
   * @param default_expr default expression
   * @param check_expr check expression
   * @param varlen size of column if varlen
   */
  ColumnDefinition(std::string name, DataType type, bool is_primary, bool is_not_null, bool is_unique,
                   common::ManagedPointer<AbstractExpression> default_expr,
                   common::ManagedPointer<AbstractExpression> check_expr, size_t varlen)
      : name_(std::move(name)),
        type_(type),
        is_primary_(is_primary),
        is_not_null_(is_not_null),
        is_unique_(is_unique),
        default_expr_(default_expr),
        check_expr_(check_expr),
        varlen_(varlen) {}

  /**
   * @param str type string
   * @return data type
   */
  static DataType StrToDataType(const char *str) {
    DataType data_type;
    // Transform column type
    if ((strcmp(str, "int") == 0) || (strcmp(str, "int4") == 0)) {
      data_type = ColumnDefinition::DataType::INT;
    } else if (strcmp(str, "varchar") == 0) {
      data_type = ColumnDefinition::DataType::VARCHAR;
    } else if (strcmp(str, "int8") == 0) {
      data_type = ColumnDefinition::DataType::BIGINT;
    } else if (strcmp(str, "int2") == 0) {
      data_type = ColumnDefinition::DataType::SMALLINT;
    } else if (strcmp(str, "timestamp") == 0) {
      data_type = ColumnDefinition::DataType::TIMESTAMP;
    } else if (strcmp(str, "bool") == 0) {
      data_type = ColumnDefinition::DataType::BOOLEAN;
    } else if (strcmp(str, "bpchar") == 0) {
      data_type = ColumnDefinition::DataType::CHAR;
    } else if ((strcmp(str, "double") == 0) || (strcmp(str, "float8") == 0)) {
      data_type = ColumnDefinition::DataType::DOUBLE;
    } else if ((strcmp(str, "real") == 0) || (strcmp(str, "float4") == 0)) {
      data_type = ColumnDefinition::DataType::FLOAT;
    } else if (strcmp(str, "numeric") == 0) {
      data_type = ColumnDefinition::DataType::DECIMAL;
    } else if (strcmp(str, "text") == 0) {
      data_type = ColumnDefinition::DataType::TEXT;
    } else if (strcmp(str, "tinyint") == 0) {
      data_type = ColumnDefinition::DataType::TINYINT;
    } else if (strcmp(str, "varbinary") == 0) {
      data_type = ColumnDefinition::DataType::VARBINARY;
    } else if (strcmp(str, "date") == 0) {
      data_type = ColumnDefinition::DataType::DATE;
    } else {
      PARSER_LOG_DEBUG("StrToDataType: Unsupported datatype: {}", str);
      throw PARSER_EXCEPTION("Unsupported datatype");
    }
    return data_type;
  }

  /**
   * @param str type name
   * @return type ID
   */
  static type::TypeId StrToValueType(char *str) {
    type::TypeId value_type;
    // Transform column type
    if ((strcmp(str, "int") == 0) || (strcmp(str, "int4") == 0)) {
      value_type = type::TypeId::INTEGER;
    } else if ((strcmp(str, "varchar") == 0) || (strcmp(str, "bpchar") == 0) || (strcmp(str, "text") == 0)) {
      value_type = type::TypeId::VARCHAR;
    } else if (strcmp(str, "int8") == 0) {
      value_type = type::TypeId::BIGINT;
    } else if (strcmp(str, "int2") == 0) {
      value_type = type::TypeId::SMALLINT;
    } else if (strcmp(str, "timestamp") == 0) {
      value_type = type::TypeId::TIMESTAMP;
    } else if (strcmp(str, "bool") == 0) {
      value_type = type::TypeId::BOOLEAN;
    } else if ((strcmp(str, "double") == 0) || (strcmp(str, "float8") == 0) || (strcmp(str, "real") == 0) ||
               (strcmp(str, "float4") == 0) || (strcmp(str, "numeric") == 0)) {
      value_type = type::TypeId::DECIMAL;
    } else if (strcmp(str, "tinyint") == 0) {
      value_type = type::TypeId::TINYINT;
    } else if (strcmp(str, "varbinary") == 0) {
      value_type = type::TypeId::VARBINARY;
    } else if (strcmp(str, "date") == 0) {
      value_type = type::TypeId::DATE;
    } else {
      PARSER_LOG_DEBUG("StrToValueType: Unsupported datatype: {}", str);
      throw PARSER_EXCEPTION("Unsupported datatype");
    }
    return value_type;
  }

  /**
   * @return type ID
   */
  type::TypeId GetValueType() {
    switch (type_) {
      case DataType::INT:
      case DataType::INTEGER:
        return type::TypeId::INTEGER;
      case DataType::TINYINT:
        return type::TypeId::TINYINT;
      case DataType::SMALLINT:
        return type::TypeId::SMALLINT;
      case DataType::BIGINT:
        return type::TypeId::BIGINT;

      case DataType::DECIMAL:
      case DataType::DOUBLE:
      case DataType::FLOAT:
        return type::TypeId::DECIMAL;

      case DataType::BOOLEAN:
        return type::TypeId::BOOLEAN;

        // case ADDRESS:
        //  return type::Type::ADDRESS;

      case DataType::TIMESTAMP:
        return type::TypeId::TIMESTAMP;

      case DataType::CHAR:
      case DataType::TEXT:
      case DataType::VARCHAR:
        return type::TypeId::VARCHAR;

      case DataType::VARBINARY:
        return type::TypeId::VARBINARY;

      case DataType::DATE:
        return type::TypeId::DATE;

      case DataType::INVALID:
      case DataType::PRIMARY:
      case DataType::FOREIGN:
      case DataType::MULTIUNIQUE:
      default:
        return type::TypeId::INVALID;
    }
  }

  /** @return column name */
  std::string GetColumnName() { return name_; }

  /** @return table information */
  common::ManagedPointer<TableInfo> GetTableInfo() { return common::ManagedPointer(table_info_); }

  /** @return column data type */
  DataType GetColumnType() { return type_; }

  /** @return true if primary key */
  bool IsPrimaryKey() { return is_primary_; }

  /** @return true if column nullable */
  bool IsNullable() { return !is_not_null_; }

  /** @return true if column should be unique */
  bool IsUnique() { return is_unique_; }

  /** @return default expression */
  common::ManagedPointer<AbstractExpression> GetDefaultExpression() { return default_expr_; }

  /** @return check expression */
  common::ManagedPointer<AbstractExpression> GetCheckExpression() { return check_expr_; }

  /** @return varlen size */
  size_t GetVarlenSize() { return varlen_; }

  /** @return foreign key sources */
  std::vector<std::string> GetForeignKeySources() { return fk_sources_; }

  /** @return foreign key sinks */
  std::vector<std::string> GetForeignKeySinks() { return fk_sinks_; }

  /** @return foreign key sink table name */
  std::string GetForeignKeySinkTableName() { return fk_sink_table_name_; }

  /** @return foreign key delete action */
  FKConstrActionType GetForeignKeyDeleteAction() { return fk_delete_action_; }

  /** @return foreign key update action */
  FKConstrActionType GetForeignKeyUpdateAction() { return fk_update_action_; }

  /** @return foreign key match type */
  FKConstrMatchType GetForeignKeyMatchType() { return fk_match_type_; }

  /** @param b true if should be primary key, false otherwise */
  void SetPrimary(bool b) { is_primary_ = b; }

  /**
   * Hashes the current column Definition
   */
  common::hash_t Hash() const {
    common::hash_t hash = common::HashUtil::Hash(name_);
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(table_info_));
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(type_));
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(static_cast<char>(is_primary_)));
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(static_cast<char>(is_not_null_)));
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(static_cast<char>(is_unique_)));
    if (default_expr_ != nullptr) hash = common::HashUtil::CombineHashes(hash, default_expr_->Hash());
    if (check_expr_ != nullptr) hash = common::HashUtil::CombineHashes(hash, check_expr_->Hash());
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(varlen_));
    hash = common::HashUtil::CombineHashInRange(hash, fk_sources_.begin(), fk_sources_.end());
    hash = common::HashUtil::CombineHashInRange(hash, fk_sinks_.begin(), fk_sinks_.end());
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(fk_sink_table_name_));
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(fk_delete_action_));
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(fk_update_action_));
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(fk_match_type_));
    return hash;
  }

  /**
   * Logical equality check.
   * @param rhs other
   * @return true if the two column definitions are logically equal
   */
  bool operator==(const ColumnDefinition &rhs) const {
    if (name_ != rhs.name_) return false;
    if (type_ != rhs.type_) return false;
    if ((!table_info_ && rhs.table_info_) || (table_info_ && table_info_ != rhs.table_info_)) return false;
    if (is_primary_ != rhs.is_primary_) return false;
    if (is_not_null_ != rhs.is_not_null_) return false;
    if (is_unique_ != rhs.is_unique_) return false;
    if (varlen_ != rhs.varlen_) return false;
    if ((!default_expr_ && rhs.default_expr_) || (default_expr_ && default_expr_ != rhs.default_expr_)) return false;
    if ((!check_expr_ && rhs.check_expr_) || (check_expr_ && check_expr_ != rhs.check_expr_)) return false;
    if (fk_sources_.size() != rhs.fk_sources_.size()) return false;
    for (size_t i = 0; i < fk_sources_.size(); i++)
      if (fk_sources_[i] != rhs.fk_sources_[i]) return false;
    if (fk_sinks_.size() != rhs.fk_sinks_.size()) return false;
    for (size_t i = 0; i < fk_sinks_.size(); i++)
      if (fk_sinks_[i] != rhs.fk_sinks_[i]) return false;
    if (fk_sink_table_name_ != rhs.fk_sink_table_name_) return false;
    if (fk_delete_action_ != rhs.fk_delete_action_) return false;
    return fk_match_type_ == rhs.fk_match_type_;
  }

  /**
   * Logical inequality check.
   * @param rhs other
   * @return true if the two column definitions are logically not equal
   */
  bool operator!=(const ColumnDefinition &rhs) const { return !operator==(rhs); }

 private:
  const std::string name_;
  const std::unique_ptr<TableInfo> table_info_ = nullptr;

  const DataType type_;
  bool is_primary_ = false;  // not const because of how the parser returns us columns and primary key info separately
  const bool is_not_null_ = false;
  const bool is_unique_ = false;
  common::ManagedPointer<AbstractExpression> default_expr_ = nullptr;
  common::ManagedPointer<AbstractExpression> check_expr_ = nullptr;
  const size_t varlen_ = 0;

  const std::vector<std::string> fk_sources_;
  const std::vector<std::string> fk_sinks_;
  const std::string fk_sink_table_name_;

  const FKConstrActionType fk_delete_action_ = FKConstrActionType::INVALID;
  const FKConstrActionType fk_update_action_ = FKConstrActionType::INVALID;
  const FKConstrMatchType fk_match_type_ = FKConstrMatchType::SIMPLE;
};

}  // namespace parser
}  // namespace terrier
