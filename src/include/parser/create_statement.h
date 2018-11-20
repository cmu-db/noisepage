#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "common/sql_node_visitor.h"
#include "parser/expression/abstract_expression.h"
#include "parser/parser_defs.h"
#include "parser/select_statement.h"
#include "parser/sql_statement.h"

namespace terrier {
namespace parser {

// TODO(WAN): definitely doesn't belong here, it used to be in type, where does it go?
static const uint32_t TEXT_MAX_LEN = 1000000000;

/**
 * Represents the definition of a table column.
 */
struct ColumnDefinition {
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

  ColumnDefinition(std::string name, DataType type, bool is_primary, bool is_not_null, bool is_unique,
                   std::unique_ptr<AbstractExpression> default_expr, std::unique_ptr<AbstractExpression> check_expr,
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
      throw NotImplementedException("Unsupported datatype\n");
    }
    return data_type;
  }

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
      throw NotImplementedException("Unsupported datatype\n");
    }
    return value_type;
  }

  static type::TypeId GetValueType(DataType type) {
    switch (type) {
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

  const std::string name_;
  const std::unique_ptr<TableInfo> table_info_ = nullptr;

  const DataType type_;
  bool is_primary_ = false;
  const bool is_not_null_ = false;
  const bool is_unique_ = false;
  const std::unique_ptr<AbstractExpression> default_expr_ = nullptr;
  const std::unique_ptr<AbstractExpression> check_expr_ = nullptr;
  const size_t varlen_ = 0;

  const std::vector<std::string> primary_key_;
  const std::vector<std::string> fk_sources_;
  const std::vector<std::string> fk_sinks_;
  const std::string fk_sink_table_name_;

  const std::vector<std::string> multi_unique_cols_;

  const FKConstrActionType fk_delete_action_ = FKConstrActionType::INVALID;
  const FKConstrActionType fk_update_action_ = FKConstrActionType::INVALID;
  const FKConstrMatchType fk_match_type_ = FKConstrMatchType::SIMPLE;
};

/**
 * Represents the sql "CREATE ..."
 */
class CreateStatement : public TableRefStatement {
  // TODO(WAN): just inherit from CreateStatement instead of dumping members here..
 public:
  enum CreateType { kTable, kDatabase, kIndex, kTrigger, kSchema, kView };

  // CREATE TABLE, CREATE DATABASE
  CreateStatement(std::unique_ptr<TableInfo> table_info, CreateType create_type,
                  std::vector<std::unique_ptr<ColumnDefinition>> columns,
                  std::vector<std::unique_ptr<ColumnDefinition>> foreign_keys)
      : TableRefStatement(StatementType::CREATE, std::move(table_info)),
        create_type_(create_type),
        columns_(std::move(columns)),
        foreign_keys_(std::move(foreign_keys)) {}

  // CREATE INDEX
  CreateStatement(std::unique_ptr<TableInfo> table_info, IndexType index_type, bool unique, std::string index_name,
                  std::vector<std::string> index_attrs)
      : TableRefStatement(StatementType::CREATE, std::move(table_info)),
        create_type_(kIndex),
        index_type_(index_type),
        unique_index_(unique),
        index_name_(std::move(index_name)),
        index_attrs_(std::move(index_attrs)) {}

  // CREATE SCHEMA
  CreateStatement(std::unique_ptr<TableInfo> table_info, bool if_not_exists)
      : TableRefStatement(StatementType::CREATE, std::move(table_info)),
        create_type_(kSchema),
        if_not_exists_(if_not_exists) {}

  // CREATE TRIGGER
  CreateStatement(std::unique_ptr<TableInfo> table_info, std::string trigger_name,
                  std::vector<std::string> trigger_funcnames, std::vector<std::string> trigger_args,
                  std::vector<std::string> trigger_columns, std::unique_ptr<AbstractExpression> trigger_when,
                  int16_t trigger_type)
      : TableRefStatement(StatementType::CREATE, std::move(table_info)),
        create_type_(kTrigger),
        trigger_name_(std::move(trigger_name)),
        trigger_funcnames_(std::move(trigger_funcnames)),
        trigger_args_(std::move(trigger_args)),
        trigger_columns_(std::move(trigger_columns)),
        trigger_when_(std::move(trigger_when)),
        trigger_type_(trigger_type) {}

  // CREATE VIEW
  CreateStatement(std::string view_name, std::unique_ptr<SelectStatement> view_query)
      : TableRefStatement(StatementType::CREATE, nullptr),
        create_type_(kView),
        view_name_(std::move(view_name)),
        view_query_(std::move(view_query)) {}

  ~CreateStatement() override = default;

  void Accept(SqlNodeVisitor *v) override { v->Visit(this); }

  // ALL
  const CreateType create_type_;

  // CREATE TABLE, CREATE DATABASE
  const std::vector<std::unique_ptr<ColumnDefinition>> columns_;
  const std::vector<std::unique_ptr<ColumnDefinition>> foreign_keys_;

  // CREATE INDEX
  const IndexType index_type_ = IndexType::INVALID;
  const bool unique_index_ = false;
  const std::string index_name_;
  const std::vector<std::string> index_attrs_;

  // CREATE SCHEMA
  const bool if_not_exists_ = false;

  // CREATE TRIGGER
  const std::string trigger_name_;
  const std::vector<std::string> trigger_funcnames_;
  const std::vector<std::string> trigger_args_;
  const std::vector<std::string> trigger_columns_;
  const std::unique_ptr<AbstractExpression> trigger_when_;
  const int16_t trigger_type_ = 0;  // information about row, timing, events, access by pg_trigger

  // TODO(WAN): this really does not belong here
  const std::string view_name_;
  const std::unique_ptr<SelectStatement> view_query_;
};

}  // namespace parser
}  // namespace terrier
