#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "binder/sql_node_visitor.h"
#include "common/error/exception.h"
#include "loggers/parser_logger.h"
#include "parser/expression/abstract_expression.h"
#include "parser/parser_defs.h"
#include "parser/select_statement.h"
#include "parser/sql_statement.h"

namespace noisepage {
namespace parser {
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
   * @param type_modifier max length of varlen, or precision of decimal (atttypmod)
   */
  ColumnDefinition(std::string name, DataType type, bool is_primary, bool is_not_null, bool is_unique,
                   common::ManagedPointer<AbstractExpression> default_expr,
                   common::ManagedPointer<AbstractExpression> check_expr, int32_t type_modifier)
      : name_(std::move(name)),
        type_(type),
        is_primary_(is_primary),
        is_not_null_(is_not_null),
        is_unique_(is_unique),
        default_expr_(default_expr),
        check_expr_(check_expr),
        type_modifier_(type_modifier) {}

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
               (strcmp(str, "float4") == 0) || (strcmp(str, "numeric") == 0) || (strcmp(str, "decimal") == 0)) {
      value_type = type::TypeId::REAL;
      // TODO(Matt): when we support fixed point DECIMAL properly:

      //    } else if ((strcmp(str, "numeric") == 0) || (strcmp(str, "decimal") == 0)) {
      //      value_type = type::TypeId::DECIMAL;
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
        // TODO(Matt): when we support fixed point DECIMAL properly:

        //        return type::TypeId::DECIMAL;
      case DataType::DOUBLE:
      case DataType::FLOAT:
        return type::TypeId::REAL;

      case DataType::BOOLEAN:
        return type::TypeId::BOOLEAN;

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

  /** @return type modifier, max varlen size or precision for DECIMAL */
  int32_t GetTypeModifier() { return type_modifier_; }

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
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(type_modifier_));
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
    if (type_modifier_ != rhs.type_modifier_) return false;
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
  const int32_t type_modifier_ = -1;

  const std::vector<std::string> fk_sources_;
  const std::vector<std::string> fk_sinks_;
  const std::string fk_sink_table_name_;

  const FKConstrActionType fk_delete_action_ = FKConstrActionType::INVALID;
  const FKConstrActionType fk_update_action_ = FKConstrActionType::INVALID;
  const FKConstrMatchType fk_match_type_ = FKConstrMatchType::SIMPLE;
};

/**
 * Represents an index attribute.
 */
class IndexAttr {
 public:
  /** Create an index attribute on a column name. */
  explicit IndexAttr(std::string name) : has_expr_(false), name_(std::move(name)), expr_(nullptr) {}

  /** Create an index attribute on an expression. */
  explicit IndexAttr(common::ManagedPointer<AbstractExpression> expr) : has_expr_(true), name_(""), expr_(expr) {}

  /** @return if the index attribute contains expression */
  bool HasExpr() const { return has_expr_; }

  /** @return the name of the column that we're indexed on */
  std::string GetName() const {
    NOISEPAGE_ASSERT(expr_ == nullptr, "Expressions don't come with names.");
    return name_;
  }

  /** @return the expression that we're indexed on */
  common::ManagedPointer<AbstractExpression> GetExpression() const { return expr_; }

 private:
  bool has_expr_;
  std::string name_;
  common::ManagedPointer<AbstractExpression> expr_;
};

/**
 * Represents the sql "CREATE ..."
 */
class CreateStatement : public TableRefStatement {
  // TODO(WAN): just inherit from CreateStatement instead of dumping members here..
 public:
  /**
   * Create statement type.
   */
  enum CreateType { kTable, kDatabase, kIndex, kTrigger, kSchema, kView };

  /**
   * CREATE TABLE and CREATE DATABASE
   * @param table_info table information
   * @param create_type create type, must be either kTable or kDatabase
   * @param columns columns to be created
   * @param foreign_keys foreign keys to be created
   */
  CreateStatement(std::unique_ptr<TableInfo> table_info, CreateType create_type,
                  std::vector<std::unique_ptr<ColumnDefinition>> columns,
                  std::vector<std::unique_ptr<ColumnDefinition>> foreign_keys)
      : TableRefStatement(StatementType::CREATE, std::move(table_info)),
        create_type_(create_type),
        columns_(std::move(columns)),
        foreign_keys_(std::move(foreign_keys)) {}

  /**
   * CREATE INDEX
   * @param table_info table information
   * @param index_type index type
   * @param unique true if index should be unique, false otherwise
   * @param index_name index name
   * @param index_attrs index attributes
   */
  CreateStatement(std::unique_ptr<TableInfo> table_info, IndexType index_type, bool unique, std::string index_name,
                  std::vector<IndexAttr> index_attrs)
      : TableRefStatement(StatementType::CREATE, std::move(table_info)),
        create_type_(kIndex),
        index_type_(index_type),
        unique_index_(unique),
        index_name_(std::move(index_name)),
        index_attrs_(std::move(index_attrs)) {}

  /**
   * CREATE SCHEMA
   * @param table_info table information
   * @param if_not_exists true if "IF NOT EXISTS" was used, false otherwise
   */
  CreateStatement(std::unique_ptr<TableInfo> table_info, bool if_not_exists)
      : TableRefStatement(StatementType::CREATE, std::move(table_info)),
        create_type_(kSchema),
        if_not_exists_(if_not_exists) {}

  /**
   * CREATE TRIGGER
   * @param table_info table information
   * @param trigger_name trigger name
   * @param trigger_funcnames trigger function names
   * @param trigger_args trigger arguments
   * @param trigger_columns trigger columns
   * @param trigger_when trigger when clause
   * @param trigger_type trigger type
   */
  CreateStatement(std::unique_ptr<TableInfo> table_info, std::string trigger_name,
                  std::vector<std::string> trigger_funcnames, std::vector<std::string> trigger_args,
                  std::vector<std::string> trigger_columns, common::ManagedPointer<AbstractExpression> trigger_when,
                  int16_t trigger_type)
      : TableRefStatement(StatementType::CREATE, std::move(table_info)),
        create_type_(kTrigger),
        trigger_name_(std::move(trigger_name)),
        trigger_funcnames_(std::move(trigger_funcnames)),
        trigger_args_(std::move(trigger_args)),
        trigger_columns_(std::move(trigger_columns)),
        trigger_when_(trigger_when),
        trigger_type_(trigger_type) {}

  /**
   * CREATE VIEW
   * @param view_name view name
   * @param view_query query associated with view
   */
  CreateStatement(std::string view_name, std::unique_ptr<SelectStatement> view_query)
      : TableRefStatement(StatementType::CREATE, nullptr),
        create_type_(kView),
        view_name_(std::move(view_name)),
        view_query_(std::move(view_query)) {}

  ~CreateStatement() override = default;

  void Accept(common::ManagedPointer<binder::SqlNodeVisitor> v) override { v->Visit(common::ManagedPointer(this)); }

  /** @return the type of create statement */
  CreateType GetCreateType() { return create_type_; }

  /** @return columns for [CREATE TABLE, CREATE DATABASE] */
  std::vector<common::ManagedPointer<ColumnDefinition>> GetColumns() {
    std::vector<common::ManagedPointer<ColumnDefinition>> cols;
    cols.reserve(columns_.size());
    for (const auto &col : columns_) {
      cols.emplace_back(common::ManagedPointer(col));
    }
    return cols;
  }

  /** @return foreign keys for [CREATE TABLE, CREATE DATABASE] */
  std::vector<common::ManagedPointer<ColumnDefinition>> GetForeignKeys() {
    std::vector<common::ManagedPointer<ColumnDefinition>> foreign_keys;
    foreign_keys.reserve(foreign_keys_.size());
    for (const auto &fk : foreign_keys_) {
      foreign_keys.emplace_back(common::ManagedPointer(fk));
    }
    return foreign_keys;
  }

  /** @return index type for [CREATE INDEX] */
  IndexType GetIndexType() { return index_type_; }

  /** @return true if index should be unique for [CREATE INDEX] */
  bool IsUniqueIndex() { return unique_index_; }

  /** @return index name for [CREATE INDEX] */
  std::string GetIndexName() { return index_name_; }

  /** @return index attributes for [CREATE INDEX] */
  const std::vector<IndexAttr> &GetIndexAttributes() const { return index_attrs_; }

  /** @return true if "IF NOT EXISTS" for [CREATE SCHEMA], false otherwise */
  bool IsIfNotExists() { return if_not_exists_; }

  /** @return trigger name for [CREATE TRIGGER] */
  std::string GetTriggerName() { return trigger_name_; }

  /** @return trigger function names for [CREATE TRIGGER] */
  std::vector<std::string> GetTriggerFuncNames() { return trigger_funcnames_; }

  /** @return trigger args for [CREATE TRIGGER] */
  std::vector<std::string> GetTriggerArgs() { return trigger_args_; }

  /** @return trigger columns for [CREATE TRIGGER] */
  std::vector<std::string> GetTriggerColumns() { return trigger_columns_; }

  /** @return trigger when clause for [CREATE TRIGGER] */
  common::ManagedPointer<AbstractExpression> GetTriggerWhen() { return common::ManagedPointer(trigger_when_); }

  /** @return trigger type, i.e. information about row, timing, events, access by pg_trigger */
  int16_t GetTriggerType() { return trigger_type_; }

  /** @return view name for [CREATE VIEW] */
  std::string GetViewName() { return view_name_; }

  /** @return view query for [CREATE VIEW] */
  common::ManagedPointer<SelectStatement> GetViewQuery() { return common::ManagedPointer(view_query_); }

 private:
  // ALL
  const CreateType create_type_;

  // CREATE TABLE, CREATE DATABASE
  const std::vector<std::unique_ptr<ColumnDefinition>> columns_;
  const std::vector<std::unique_ptr<ColumnDefinition>> foreign_keys_;

  // CREATE INDEX
  const IndexType index_type_ = IndexType::INVALID;
  const bool unique_index_ = false;
  const std::string index_name_;
  const std::vector<IndexAttr> index_attrs_;

  // CREATE SCHEMA
  const bool if_not_exists_ = false;

  // CREATE TRIGGER
  const std::string trigger_name_;
  const std::vector<std::string> trigger_funcnames_;
  const std::vector<std::string> trigger_args_;
  const std::vector<std::string> trigger_columns_;
  const common::ManagedPointer<AbstractExpression> trigger_when_ = common::ManagedPointer<AbstractExpression>(nullptr);
  const int16_t trigger_type_ = 0;

  // CREATE VIEW
  const std::string view_name_;
  const std::unique_ptr<SelectStatement> view_query_;
};

}  // namespace parser
}  // namespace noisepage
