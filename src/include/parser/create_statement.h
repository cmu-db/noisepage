#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "binder/sql_node_visitor.h"
#include "common/exception.h"
#include "loggers/parser_logger.h"
#include "parser/expression/abstract_expression.h"
#include "parser/parser_defs.h"
#include "parser/select_statement.h"
#include "parser/sql_statement.h"

namespace terrier {
namespace parser {

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
    TERRIER_ASSERT(expr_ == nullptr, "Expressions don't come with names.");
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
}  // namespace terrier
