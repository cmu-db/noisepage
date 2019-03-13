#pragma once

#include "abstract_plan_node.h"
#include "catalog/schema.h"
#include "parser/create_statement.h"

namespace terrier {
namespace parser {
class CreateStatement;
class AbstractExpression;
}  // namespace parser

namespace plan_node {

/**
 * The meta-data for a constraint reference.
 * This is meant to be a bridge from the parser to the
 * catalog. It only has table names and not OIDs, whereas
 * the catalog only wants OIDs.
 */
struct PrimaryKeyInfo {
  std::vector<std::string> primary_key_cols;
  std::string constraint_name;
};

struct ForeignKeyInfo {
  std::vector<std::string> foreign_key_sources;
  std::vector<std::string> foreign_key_sinks;
  std::string sink_table_name;
  std::string constraint_name;
  parser::FKConstrActionType upd_action;
  parser::FKConstrActionType del_action;
};

struct UniqueInfo {
  std::vector<std::string> unique_cols;
  std::string constraint_name;
};

struct CheckInfo {
  std::vector<std::string> check_cols;
  std::string constraint_name;
  parser::ExpressionType expr_type;
  type::TransientValue expr_value;

  /**
   * CheckInfo constructor
   * @param check_cols name of the columns to be checked
   * @param constraint_name name of the constraint
   * @param expr_type the type of the expression to be satisfied
   * @param expr_value the value of the expression to be satisfied
   */
  CheckInfo(std::vector<std::string> check_cols, std::string constraint_name, parser::ExpressionType expr_type,
            type::TransientValue expr_value)
      : check_cols(std::move(check_cols)),
        constraint_name(std::move(constraint_name)),
        expr_type(expr_type),
        expr_value(std::move(expr_value)) {}
};

class CreatePlanNode : public AbstractPlanNode {
 public:
  CreatePlanNode() = delete;
  /**
   * This construnctor is for Create Database Test used only
   * @param database_name name of the database to create new objects in
   * @param c_type type of object to be created
   */
  explicit CreatePlanNode(std::string database_name, CreateType c_type);

  /**
   * This construnctor is for copy() used only
   * @param table_name name of the table to be created
   * @param schema_name name of the schema to be created
   * @param database_name name of the database to create new objects in
   * @param schema schema of new table
   * @param c_type type of object to be created
   */
  explicit CreatePlanNode(std::string table_name, std::string schema_name, std::string database_name,
                          std::shared_ptr<catalog::Schema> schema, CreateType c_type);

  /**
   * Instantiate new CreatePlanNode
   * @param create_stmt the CREATE statement from parser
   */
  explicit CreatePlanNode(parser::CreateStatement *create_stmt);

  /**
   * @return the type of this plan node
   */
  inline PlanNodeType GetPlanNodeType() const override { return PlanNodeType::CREATE; }

  /**
   * @return debug info
   */
  const std::string GetInfo() const { return "Create Plan Node"; }

  /**
   * @return unique pointer to a copy of this plan node
   */
  std::unique_ptr<AbstractPlanNode> Copy() const override {
    return std::unique_ptr<AbstractPlanNode>(
        new CreatePlanNode(table_name_, schema_name_, database_name_, table_schema_, create_type_));
  }

  /**
   * @return name of the index for [CREATE INDEX]
   */
  std::string GetIndexName() const { return index_name_; }
  /**
   * @return name of the table for [CREATE TABLE]
   */
  std::string GetTableName() const { return table_name_; }
  /**
   * @return name of the schema for [CREATE SCHEMA]
   */
  std::string GetSchemaName() const { return schema_name_; }

  /**
   * @return name of the database for [CREATE DATABASE]
   */
  std::string GetDatabaseName() const { return database_name_; }

  /**
   * @return pointer to the schema for [CREATE TABLE]
   */
  std::shared_ptr<catalog::Schema> GetSchema() const { return table_schema_; }

  /**
   * @return type of object to be created
   */
  CreateType GetCreateType() const { return create_type_; }

  /**
   * @return true if index should be unique for [CREATE INDEX]
   */
  bool IsUniqueIndex() const { return unique_index_; }

  /**
   * @return index type for [CREATE INDEX]
   */
  parser::IndexType GetIndexType() const { return index_type_; }

  /**
   * @return index attributes for [CREATE INDEX]
   */
  std::vector<std::string> GetIndexAttributes() const { return index_attrs_; }

  /**
   * @return true if index/table has primary key [CREATE INDEX/TABLE]
   */
  inline bool HasPrimaryKey() const { return has_primary_key_; }

  /**
   * @return primary key meta-data
   */
  inline PrimaryKeyInfo GetPrimaryKey() const { return primary_key_; }

  /**
   * @return foreign keys meta-data
   */
  inline std::vector<ForeignKeyInfo> GetForeignKeys() const { return foreign_keys_; }

  /**
   * @return unique constraints
   */
  inline std::vector<UniqueInfo> GetUniques() const { return con_uniques_; }

  /**
   * @return name of key attributes
   */
  std::vector<std::string> GetKeyAttrs() const { return key_attrs_; }

  /**
   * Set names of key attributes
   * @param p_key_attrs names of key attributes to be set
   */
  void SetKeyAttrs(std::vector<std::string> p_key_attrs) { key_attrs_ = std::move(p_key_attrs); }

  /**
   * @return trigger name for [CREATE TRIGGER]
   */
  std::string GetTriggerName() const { return trigger_name_; }

  /**
   * @return trigger function names for [CREATE TRIGGER]
   */
  std::vector<std::string> GetTriggerFuncName() const { return trigger_funcnames_; }

  /**
   * @return trigger args for [CREATE TRIGGER]
   */
  std::vector<std::string> GetTriggerArgs() const { return trigger_args_; }

  /**
   * @return trigger columns for [CREATE TRIGGER]
   */
  std::vector<std::string> GetTriggerColumns() const { return trigger_columns_; }

  /**
   * @return trigger when clause for [CREATE TRIGGER]
   */
  std::shared_ptr<parser::AbstractExpression> GetTriggerWhen() const { return trigger_when_; }

  /**
   * @return trigger type, i.e. information about row, timing, events, access by pg_trigger
   */
  int16_t GetTriggerType() const { return trigger_type_; }

 protected:
  /**
   * Extract foreign key constraints from column definition
   * @param table_name table to be created
   * @param col column definitions
   */
  void ProcessForeignKeyConstraint(const std::string &table_name, const std::shared_ptr<parser::ColumnDefinition> &col);

  /**
   * Extract unique constraints
   * @param col column definition
   */
  void ProcessUniqueConstraint(const std::shared_ptr<parser::ColumnDefinition> &col);

  /**
   * Extract check constraints
   * @param col column definition
   */
  void ProcessCheckConstraint(const std::shared_ptr<parser::ColumnDefinition> &col);

 private:
  // Table Name
  std::string table_name_;

  // namespace Name
  std::string schema_name_;

  // Database Name
  std::string database_name_;

  // Table Schema
  std::shared_ptr<catalog::Schema> table_schema_;

  // Type of object to create
  CreateType create_type_;

  // CREATE INDEX
  parser::IndexType index_type_ = parser::IndexType::INVALID;
  bool unique_index_ = false;
  std::string index_name_;
  std::vector<std::string> index_attrs_;
  std::vector<std::string> key_attrs_;

  // ColumnDefinition for multi-column constraints (including foreign key)
  bool has_primary_key_ = false;
  PrimaryKeyInfo primary_key_;
  std::vector<ForeignKeyInfo> foreign_keys_;
  std::vector<UniqueInfo> con_uniques_;
  std::vector<CheckInfo> con_checks_;

  // CREATE TRIGGER
  std::string trigger_name_;
  std::vector<std::string> trigger_funcnames_;
  std::vector<std::string> trigger_args_;
  std::vector<std::string> trigger_columns_;
  std::shared_ptr<parser::AbstractExpression> trigger_when_;
  int16_t trigger_type_ = 0;

 private:
  DISALLOW_COPY_AND_MOVE(CreatePlanNode);
};

}  // namespace plan_node
}  // namespace terrier