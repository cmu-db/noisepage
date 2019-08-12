#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "common/exception.h"
#include "common/sql_node_visitor.h"
#include "loggers/parser_logger.h"
#include "parser/expression/abstract_expression.h"
#include "parser/parser_defs.h"
#include "parser/select_statement.h"
#include "parser/sql_statement.h"

namespace terrier {
namespace parser {

/**
 * Represents the definition of a table column.
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
                   std::shared_ptr<AbstractExpression> default_expr, std::shared_ptr<AbstractExpression> check_expr,
                   size_t varlen)
      : name_(std::move(name)),
        type_(type),
        is_primary_(is_primary),
        is_not_null_(is_not_null),
        is_unique_(is_unique),
        default_expr_(std::move(default_expr)),
        check_expr_(std::move(check_expr)),
        varlen_(varlen) {}

  virtual ~ColumnDefinition() = default;

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

  /**
   * @return column name
   */
  std::string GetColumnName() { return name_; }

  /**
   * @return table information
   */
  std::shared_ptr<TableInfo> GetTableInfo() { return table_info_; }

  /**
   * @return column data type
   */
  DataType GetColumnType() { return type_; }

  /**
   * @return true if primary key
   */
  bool IsPrimaryKey() { return is_primary_; }

  /**
   * @return true if column nullable
   */
  bool IsNullable() { return !is_not_null_; }

  /**
   * @return true if column should be unique
   */
  bool IsUnique() { return is_unique_; }

  /**
   * @return default expression
   */
  std::shared_ptr<AbstractExpression> GetDefaultExpression() { return default_expr_; }

  /**
   * @return check expression
   */
  std::shared_ptr<AbstractExpression> GetCheckExpression() { return check_expr_; }

  /**
   * @return varlen size
   */
  size_t GetVarlenSize() { return varlen_; }

  /**
   * @return foreign key sources
   */
  std::vector<std::string> GetForeignKeySources() { return fk_sources_; }

  /**
   * @return foreign key sinks
   */
  std::vector<std::string> GetForeignKeySinks() { return fk_sinks_; }

  /**
   * @return foreign key sink table name
   */
  std::string GetForeignKeySinkTableName() { return fk_sink_table_name_; }

  /**
   * @return foreign key delete action
   */
  FKConstrActionType GetForeignKeyDeleteAction() { return fk_delete_action_; }

  /**
   * @return foreign key update action
   */
  FKConstrActionType GetForeignKeyUpdateAction() { return fk_update_action_; }

  /**
   * @return foreign key match type
   */
  FKConstrMatchType GetForeignKeyMatchType() { return fk_match_type_; }

  /**
   * @param b true if should be primary key, false otherwise
   */
  void SetPrimary(bool b) { is_primary_ = b; }

 private:
  const std::string name_;
  const std::shared_ptr<TableInfo> table_info_ = nullptr;

  const DataType type_;
  bool is_primary_ = false;  // not const because of how the parser returns us columns and primary key info separately
  const bool is_not_null_ = false;
  const bool is_unique_ = false;
  const std::shared_ptr<AbstractExpression> default_expr_ = nullptr;
  const std::shared_ptr<AbstractExpression> check_expr_ = nullptr;
  const size_t varlen_ = 0;

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
  /**
   * Create an index attribute on a column name.
   */
  explicit IndexAttr(std::string name) : name_(std::move(name)), expr_(nullptr) {}

  /**
   * Create an index attribute on an expression.
   */
  explicit IndexAttr(std::shared_ptr<AbstractExpression> expr) : name_(""), expr_(std::move(expr)) {}

  /**
   * @return the name of the column that we're indexed on
   */
  std::string GetName() const {
    TERRIER_ASSERT(expr_ == nullptr, "Expressions don't come with names.");
    return name_;
  }

  /**
   * @return the expression that we're indexed on
   */
  std::shared_ptr<AbstractExpression> GetExpression() const {
    TERRIER_ASSERT(expr_ != nullptr, "Names don't come with expressions.");
    return expr_;
  }

 private:
  const std::string name_;
  const std::shared_ptr<AbstractExpression> expr_;
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
  CreateStatement(std::shared_ptr<TableInfo> table_info, CreateType create_type,
                  std::vector<std::shared_ptr<ColumnDefinition>> columns,
                  std::vector<std::shared_ptr<ColumnDefinition>> foreign_keys)
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
  CreateStatement(std::shared_ptr<TableInfo> table_info, IndexType index_type, bool unique, std::string index_name,
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
  CreateStatement(std::shared_ptr<TableInfo> table_info, bool if_not_exists)
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
  CreateStatement(std::shared_ptr<TableInfo> table_info, std::string trigger_name,
                  std::vector<std::string> trigger_funcnames, std::vector<std::string> trigger_args,
                  std::vector<std::string> trigger_columns, std::shared_ptr<AbstractExpression> trigger_when,
                  int16_t trigger_type)
      : TableRefStatement(StatementType::CREATE, std::move(table_info)),
        create_type_(kTrigger),
        trigger_name_(std::move(trigger_name)),
        trigger_funcnames_(std::move(trigger_funcnames)),
        trigger_args_(std::move(trigger_args)),
        trigger_columns_(std::move(trigger_columns)),
        trigger_when_(std::move(trigger_when)),
        trigger_type_(trigger_type) {}

  /**
   * CREATE VIEW
   * @param view_name view name
   * @param view_query query associated with view
   */
  CreateStatement(std::string view_name, std::shared_ptr<SelectStatement> view_query)
      : TableRefStatement(StatementType::CREATE, nullptr),
        create_type_(kView),
        view_name_(std::move(view_name)),
        view_query_(std::move(view_query)) {}

  ~CreateStatement() override = default;

  void Accept(SqlNodeVisitor *v) override { v->Visit(this); }

  /**
   * @return the type of create statement
   */
  CreateType GetCreateType() { return create_type_; }

  /**
   * @return columns for [CREATE TABLE, CREATE DATABASE]
   */
  std::vector<std::shared_ptr<ColumnDefinition>> GetColumns() { return columns_; }

  /**
   * @return foreign keys for [CREATE TABLE, CREATE DATABASE]
   */
  std::vector<std::shared_ptr<ColumnDefinition>> GetForeignKeys() { return foreign_keys_; }

  /**
   * @return index type for [CREATE INDEX]
   */
  IndexType GetIndexType() { return index_type_; }

  /**
   * @return true if index should be unique for [CREATE INDEX]
   */
  bool IsUniqueIndex() { return unique_index_; }

  /**
   * @return index name for [CREATE INDEX]
   */
  std::string GetIndexName() { return index_name_; }

  /**
   * @return index attributes for [CREATE INDEX]
   */
  std::vector<IndexAttr> GetIndexAttributes() { return index_attrs_; }

  /**
   * @return true if "IF NOT EXISTS" for [CREATE SCHEMA], false otherwise
   */
  bool IsIfNotExists() { return if_not_exists_; }

  /**
   * @return trigger name for [CREATE TRIGGER]
   */
  std::string GetTriggerName() { return trigger_name_; }

  /**
   * @return trigger function names for [CREATE TRIGGER]
   */
  std::vector<std::string> GetTriggerFuncNames() { return trigger_funcnames_; }

  /**
   * @return trigger args for [CREATE TRIGGER]
   */
  std::vector<std::string> GetTriggerArgs() { return trigger_args_; }

  /**
   * @return trigger columns for [CREATE TRIGGER]
   */
  std::vector<std::string> GetTriggerColumns() { return trigger_columns_; }

  /**
   * @return trigger when clause for [CREATE TRIGGER]
   */
  std::shared_ptr<AbstractExpression> GetTriggerWhen() { return trigger_when_; }

  /**
   * @return trigger type, i.e. information about row, timing, events, access by pg_trigger
   */
  int16_t GetTriggerType() { return trigger_type_; }

  /**
   * @return view name for [CREATE VIEW]
   */
  std::string GetViewName() { return view_name_; }

  /**
   * @return view query for [CREATE VIEW]
   */
  std::shared_ptr<SelectStatement> GetViewQuery() { return view_query_; }

 private:
  // ALL
  const CreateType create_type_;

  // CREATE TABLE, CREATE DATABASE
  const std::vector<std::shared_ptr<ColumnDefinition>> columns_;
  const std::vector<std::shared_ptr<ColumnDefinition>> foreign_keys_;

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
  const std::shared_ptr<AbstractExpression> trigger_when_;
  const int16_t trigger_type_ = 0;

  // CREATE VIEW
  const std::string view_name_;
  const std::shared_ptr<SelectStatement> view_query_;
};

}  // namespace parser
}  // namespace terrier
