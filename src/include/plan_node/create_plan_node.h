#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "catalog/schema.h"
#include "parser/create_statement.h"
#include "parser/expression/abstract_expression.h"
#include "parser/expression/constant_value_expression.h"
#include "parser/select_statement.h"
#include "plan_node/abstract_plan_node.h"

namespace terrier::plan_node {

/**
 * The meta-data for a constraint reference.
 * This is meant to be a bridge from the parser to the
 * catalog. It only has table names and not OIDs, whereas
 * the catalog only wants OIDs.
 */
struct PrimaryKeyInfo {
  std::vector<std::string> primary_key_cols_;
  std::string constraint_name_;

  /**
   * @return the hashed value of this primary key info
   */
  common::hash_t Hash() const {
    // Hash constraint_name
    common::hash_t hash = common::HashUtil::Hash(constraint_name_);

    // Hash primary_key_cols
    hash = common::HashUtil::CombineHashInRange(hash, primary_key_cols_.begin(), primary_key_cols_.end());
    return hash;
  }

  /**
   * Logical equality check.
   * @param rhs other
   * @return true if the two primary key info are logically equal
   */
  bool operator==(const PrimaryKeyInfo &rhs) const {
    if (constraint_name_ != rhs.constraint_name_) return false;

    if (primary_key_cols_.size() != rhs.primary_key_cols_.size()) return false;
    for (size_t i = 0; i < primary_key_cols_.size(); i++) {
      if (primary_key_cols_[i] != rhs.primary_key_cols_[i]) {
        return false;
      }
    }
    return true;
  }

  /**
   * Logical equality check.
   * @param rhs other
   * @return true if the two primary key info are not logically equal
   */
  bool operator!=(const PrimaryKeyInfo &rhs) const { return !(*this == rhs); }
};

struct ForeignKeyInfo {
  std::vector<std::string> foreign_key_sources_;
  std::vector<std::string> foreign_key_sinks_;
  std::string sink_table_name_;
  std::string constraint_name_;
  parser::FKConstrActionType upd_action_;
  parser::FKConstrActionType del_action_;

  /**
   * @return the hashed value of this foreign key info
   */
  common::hash_t Hash() const {
    // Hash constraint_name
    common::hash_t hash = common::HashUtil::Hash(constraint_name_);

    // Hash sink_table_name
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(sink_table_name_));

    // Hash foreign_key_sources
    hash = common::HashUtil::CombineHashInRange(hash, foreign_key_sources_.begin(), foreign_key_sources_.end());

    // Hash foreign_key_sinks
    hash = common::HashUtil::CombineHashInRange(hash, foreign_key_sinks_.begin(), foreign_key_sinks_.end());

    // Hash upd_action
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&upd_action_));

    // Hash del_action
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&del_action_));
    return hash;
  }

  /**
   * Logical equality check.
   * @param rhs other
   * @return true if the two check info are logically equal
   */
  bool operator==(const ForeignKeyInfo &rhs) const {
    if (constraint_name_ != rhs.constraint_name_) return false;

    if (sink_table_name_ != rhs.sink_table_name_) return false;

    if (upd_action_ != rhs.upd_action_) return false;

    if (del_action_ != rhs.del_action_) return false;

    if (foreign_key_sources_.size() != rhs.foreign_key_sources_.size()) return false;
    for (size_t i = 0; i < foreign_key_sources_.size(); i++) {
      if (foreign_key_sources_[i] != rhs.foreign_key_sources_[i]) {
        return false;
      }
    }

    if (foreign_key_sinks_.size() != rhs.foreign_key_sinks_.size()) return false;
    for (size_t i = 0; i < foreign_key_sinks_.size(); i++) {
      if (foreign_key_sinks_[i] != rhs.foreign_key_sinks_[i]) {
        return false;
      }
    }
    return true;
  }

  /**
   * Logical equality check.
   * @param rhs other
   * @return true if the two primary key info are not logically equal
   */
  bool operator!=(const ForeignKeyInfo &rhs) const { return !(*this == rhs); }
};

struct UniqueInfo {
  std::vector<std::string> unique_cols_;
  std::string constraint_name_;

  /**
   * @return the hashed value of this unique info
   */
  common::hash_t Hash() const {
    // Hash constraint_name
    common::hash_t hash = common::HashUtil::Hash(constraint_name_);

    // Hash unique_cols
    hash = common::HashUtil::CombineHashInRange(hash, unique_cols_.begin(), unique_cols_.end());
    return hash;
  }

  /**
   * Logical equality check.
   * @param rhs other
   * @return true if the two unique info are logically equal
   */
  bool operator==(const UniqueInfo &rhs) const {
    if (constraint_name_ != rhs.constraint_name_) return false;

    if (unique_cols_.size() != rhs.unique_cols_.size()) return false;
    for (size_t i = 0; i < unique_cols_.size(); i++) {
      if (unique_cols_[i] != rhs.unique_cols_[i]) {
        return false;
      }
    }
    return true;
  }

  /**
   * Logical equality check.
   * @param rhs other
   * @return true if the two primary key info are not logically equal
   */
  bool operator!=(const UniqueInfo &rhs) const { return !(*this == rhs); }
};

struct CheckInfo {
  std::vector<std::string> check_cols_;
  std::string constraint_name_;
  parser::ExpressionType expr_type_;
  type::TransientValue expr_value_;

  /**
   * CheckInfo constructor
   * @param check_cols name of the columns to be checked
   * @param constraint_name name of the constraint
   * @param expr_type the type of the expression to be satisfied
   * @param expr_value the value of the expression to be satisfied
   */
  CheckInfo(std::vector<std::string> check_cols, std::string constraint_name, parser::ExpressionType expr_type,
            type::TransientValue expr_value)
      : check_cols_(std::move(check_cols)),
        constraint_name_(std::move(constraint_name)),
        expr_type_(expr_type),
        expr_value_(std::move(expr_value)) {}

  /**
   * @return the hashed value of this check info
   */
  common::hash_t Hash() const {
    // Hash constraint_name
    common::hash_t hash = common::HashUtil::Hash(constraint_name_);

    // Hash check_cols
    hash = common::HashUtil::CombineHashInRange(hash, check_cols_.begin(), check_cols_.end());

    // Hash expr_type
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&expr_type_));

    // Hash expr_value
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(expr_value_.Hash()));
    return hash;
  }

  /**
   * Logical equality check.
   * @param rhs other
   * @return true if the two check info are logically equal
   */
  bool operator==(const CheckInfo &rhs) const {
    if (constraint_name_ != rhs.constraint_name_) return false;

    if (expr_type_ != rhs.expr_type_) return false;

    if (expr_value_ != rhs.expr_value_) return false;

    if (check_cols_.size() != rhs.check_cols_.size()) return false;
    for (size_t i = 0; i < check_cols_.size(); i++) {
      if (check_cols_[i] != rhs.check_cols_[i]) {
        return false;
      }
    }
    return true;
  }

  /**
   * Logical equality check.
   * @param rhs other
   * @return true if the two primary key info are not logically equal
   */
  bool operator!=(const CheckInfo &rhs) const { return !(*this == rhs); }
};

class CreatePlanNode : public AbstractPlanNode {
 protected:
  /**
   * Builder for a create plan node
   */
  class Builder : public AbstractPlanNode::Builder<Builder> {
   public:
    /**
     * Dont allow builder to be copied or moved
     */
    DISALLOW_COPY_AND_MOVE(Builder);

    /**
     * @param create_type type of object to create
     * @return builder object
     */
    Builder &SetCreateType(CreateType create_type) {
      create_type_ = create_type;
      return *this;
    }

    /**
     * @param table_name the name of the table [CREATE TABLE]
     * @return builder object
     */
    Builder &SetTableName(std::string table_name) {
      table_name_ = std::move(table_name);
      return *this;
    }

    /**
     * @param schema_name the name of the schema [CREATE TABLE, CREATE SCHEMA]
     * @return builder object
     */
    Builder &SetSchemaName(std::string schema_name) {
      schema_name_ = std::move(schema_name);
      return *this;
    }

    /**
     * @param database_name the name of the database [CREATE TABLE, CREATe DATABASE
     * @return builder object
     */
    Builder &SetDatabaseName(std::string database_name) {
      database_name_ = std::move(database_name);
      return *this;
    }

    /**
     * @param table_schema the schema of the table [CREATE TABLE]
     * @return builder object
     */
    Builder &SetTableSchema(std::shared_ptr<catalog::Schema> table_schema) {
      table_schema_ = std::move(table_schema);
      return *this;
    }

    /**
     * @param index_name the name of the index for [CREATE INDEX]
     * @return builder object
     */
    Builder &SetIndexName(std::string index_name) {
      index_name_ = std::move(index_name);
      return *this;
    }

    /**
     * @param unique_index true if index should be unique for [CREATE INDEX]
     * @return builder object
     */
    Builder &SetUniqueIndex(bool unique_index) {
      unique_index_ = unique_index;
      return *this;
    }

    /**
     * @param index_attrs index attributes for [CREATE INDEX]
     * @return builder object
     */
    Builder &SetIndexAttrs(std::vector<std::string> &&index_attrs) {
      index_attrs_ = std::move(index_attrs);
      return *this;
    }

    /**
     * @param key_attrs key attributes for [CREATE INDEX]
     * @return builder object
     */
    Builder &SetKeyAttrs(std::vector<std::string> &&key_attrs) {
      key_attrs_ = key_attrs;
      return *this;
    }

    /**
     * @param has_primary_key has_primary_key true if index/table has primary key [CREATE INDEX/TABLE]
     * @return builder object
     */
    Builder &SetHasPrimaryKey(bool has_primary_key) {
      has_primary_key_ = has_primary_key;
      return *this;
    }

    /**
     * @param primary_key primary_key of table [CREATE TABLE]
     * @return builder object
     */
    Builder &SetPrimaryKey(PrimaryKeyInfo primary_key) {
      primary_key_ = std::move(primary_key);
      return *this;
    }

    /**
     * @param foreign_keys foreign keys meta-data [CREATE TABLE]
     * @return builder object
     */
    Builder &SetForeignKeys(std::vector<ForeignKeyInfo> &&foreign_keys) {
      foreign_keys_ = foreign_keys;
      return *this;
    }

    /**
     * @param con_uniques unique constraints [CREATE TABLE]
     * @return builder object
     */
    Builder &SetUniqueConstraints(std::vector<UniqueInfo> &&con_uniques) {
      con_uniques_ = std::move(con_uniques);
      return *this;
    }

    /**
     * @param con_checks check constraints [CREATE TABLE]
     * @return builder object
     */
    Builder &SetCheckConstraints(std::vector<CheckInfo> &&con_checks) {
      con_checks_ = std::move(con_checks);
      return *this;
    }

    /**
     * @param trigger_name name of the trigger [CREATE TRIGGER]
     * @return builder object
     */
    Builder &SetTriggerName(std::string trigger_name) {
      trigger_name_ = std::move(trigger_name);
      return *this;
    }

    /**
     * @param trigger_funcnames trigger function names for [CREATE TRIGGER]
     * @return builder object
     */
    Builder &SetTriggerFuncnames(std::vector<std::string> &&trigger_funcnames) {
      trigger_funcnames_ = std::move(trigger_funcnames);
      return *this;
    }

    /**
     * @param trigger_args trigger args for [CREATE TRIGGER]
     * @return builder object
     */
    Builder &SetTriggerArgs(std::vector<std::string> &&trigger_args) {
      trigger_args_ = std::move(trigger_args);
      return *this;
    }

    /**
     * @param trigger_columns trigger columns for [CREATE TRIGGER]
     * @return builder object
     */
    Builder &SetTriggerColumns(std::vector<std::string> &&trigger_columns) {
      trigger_columns_ = std::move(trigger_columns);
      return *this;
    }

    /**
     * @param trigger_when trigger when clause for [CREATE TRIGGER]
     * @return builder object
     */
    Builder &SetTriggerWhen(std::shared_ptr<parser::AbstractExpression> trigger_when) {
      trigger_when_ = std::move(trigger_when);
      return *this;
    }

    /**
     * @param trigger_type trigger type, i.e. information about row, timing, events, access by pg_trigger
     * @return builder object
     */
    Builder &SetTriggerType(int16_t trigger_type) {
      trigger_type_ = trigger_type;
      return *this;
    }

    /**
     * @param view_name  view name for [CREATE VIEW]
     * @return builder object
     */
    Builder &SetViewName(std::string view_name) {
      view_name_ = std::move(view_name);
      return *this;
    }

    /**
     * @param view_query view query for [CREATE VIEW]
     * @return builder object
     */
    Builder &SetViewQuery(std::shared_ptr<parser::SelectStatement> view_query) {
      view_query_ = std::move(view_query);
      return *this;
    }

    /**
     * @param create_stmt the SQL CREATE statement
     * @return builder object
     */
    Builder &SetFromCreateStatement(parser::CreateStatement *create_stmt) {
      switch (create_stmt->GetCreateType()) {
        case parser::CreateStatement::CreateType::kDatabase: {
          create_type_ = CreateType::DB;
          database_name_ = std::string(create_stmt->GetDatabaseName());
          break;
        }

        case parser::CreateStatement::CreateType::kSchema: {
          create_type_ = CreateType::SCHEMA;
          database_name_ = std::string(create_stmt->GetDatabaseName());
          schema_name_ = std::string(create_stmt->GetSchemaName());
          break;
        }

        case parser::CreateStatement::CreateType::kTable: {
          table_name_ = std::string(create_stmt->GetTableName());
          schema_name_ = std::string(create_stmt->GetSchemaName());
          database_name_ = std::string(create_stmt->GetDatabaseName());
          std::vector<catalog::Schema::Column> columns;
          std::vector<std::string> pri_cols;

          create_type_ = CreateType::TABLE;

          for (auto &col : create_stmt->GetColumns()) {
            type::TypeId val = col->GetValueType(col->GetColumnType());

            // Create column
            // TODO(Gus,WEN) create columns using the catalog once it is available
            auto column = catalog::Schema::Column(std::string(col->GetColumnName()), val, false, catalog::col_oid_t(0));

            // Add DEFAULT constraints to the column
            if (col->GetDefaultExpression() != nullptr) {
              // Referenced from insert_plan.cpp
              if (col->GetDefaultExpression()->GetExpressionType() != parser::ExpressionType::VALUE_PARAMETER) {
                // TODO(Gus,Wen) set default value
                // parser::ConstantValueExpression *const_expr_elem =
                //    dynamic_cast<parser::ConstantValueExpression *>(col->GetDefaultExpression().get());
                // column.SetDefaultValue(const_expr_elem->GetValue());
              }
            }

            columns.emplace_back(column);

            // Collect Multi-column constraints information

            // Primary key
            if (col->IsPrimaryKey()) {
              pri_cols.push_back(col->GetColumnName());
            }

            // Unique constraint
            // Currently only supports for single column
            if (col->IsUnique()) {
              ProcessUniqueConstraint(col);
            }

            // Check expression constraint
            // Currently only supports simple boolean forms like (a > 0)
            if (col->GetCheckExpression() != nullptr) {
              ProcessCheckConstraint(col);
            }
          }

          // The parser puts the multi-column constraint information
          // into an artificial ColumnDefinition.
          // primary key constraint
          if (!pri_cols.empty()) {
            primary_key_.primary_key_cols_ = pri_cols;
            primary_key_.constraint_name_ = "con_primary";
            has_primary_key_ = true;
          }

          // foreign key
          for (auto &fk : create_stmt->GetForeignKeys()) {
            ProcessForeignKeyConstraint(table_name_, fk);
          }

          // TODO(Gus,Wen) UNIQUE and CHECK constraints

          table_schema_ = std::make_shared<catalog::Schema>(columns);
          break;
        }
        case parser::CreateStatement::CreateType::kIndex: {
          create_type_ = CreateType::INDEX;
          index_name_ = std::string(create_stmt->GetIndexName());
          table_name_ = std::string(create_stmt->GetTableName());
          schema_name_ = std::string(create_stmt->GetSchemaName());
          database_name_ = std::string(create_stmt->GetDatabaseName());

          // This holds the attribute names.
          // This is a fix for a bug where
          // The vector<char*>* items gets deleted when passed
          // To the Executor.

          std::vector<std::string> index_attrs_holder;

          for (auto &attr : create_stmt->GetIndexAttributes()) {
            index_attrs_holder.push_back(attr);
          }

          index_attrs_ = index_attrs_holder;

          index_type_ = create_stmt->GetIndexType();

          unique_index_ = create_stmt->IsUniqueIndex();
          break;
        }

        case parser::CreateStatement::CreateType::kTrigger: {
          create_type_ = CreateType::TRIGGER;
          trigger_name_ = std::string(create_stmt->GetTriggerName());
          table_name_ = std::string(create_stmt->GetTableName());
          schema_name_ = std::string(create_stmt->GetSchemaName());
          database_name_ = std::string(create_stmt->GetDatabaseName());

          if (create_stmt->GetTriggerWhen()) {
            trigger_when_ = create_stmt->GetTriggerWhen()->Copy();
          } else {
            trigger_when_ = nullptr;
          }
          trigger_type_ = create_stmt->GetTriggerType();

          for (auto &s : create_stmt->GetTriggerFuncNames()) {
            trigger_funcnames_.push_back(s);
          }
          for (auto &s : create_stmt->GetTriggerArgs()) {
            trigger_args_.push_back(s);
          }
          for (auto &s : create_stmt->GetTriggerColumns()) {
            trigger_columns_.push_back(s);
          }

          break;
        }
        default:
          break;
      }
      return *this;
    }

    /**
     * Extract foreign key constraints from column definition
     * @param table_name name of the table to get foreign key constraints
     * @param col multi-column constraint definition
     * @return builder object
     */
    Builder &ProcessForeignKeyConstraint(const std::string &table_name,
                                         const std::shared_ptr<parser::ColumnDefinition> &col) {
      ForeignKeyInfo fkey_info;

      fkey_info.foreign_key_sources_ = std::vector<std::string>();
      fkey_info.foreign_key_sinks_ = std::vector<std::string>();

      // Extract source and sink column names
      for (auto &key : col->GetForeignKeySources()) {
        fkey_info.foreign_key_sources_.push_back(key);
      }
      for (auto &key : col->GetForeignKeySinks()) {
        fkey_info.foreign_key_sinks_.push_back(key);
      }

      // Extract table names
      fkey_info.sink_table_name_ = col->GetForeignKeySinkTableName();

      // Extract delete and update actions
      fkey_info.upd_action_ = col->GetForeignKeyUpdateAction();
      fkey_info.del_action_ = col->GetForeignKeyDeleteAction();

      fkey_info.constraint_name_ = "FK_" + table_name + "->" + fkey_info.sink_table_name_;

      foreign_keys_.push_back(fkey_info);
      return *this;
    }

    /**
     * Extract unique constraints
     * @param col multi-column constraint definition
     * @return builder object
     */
    Builder &ProcessUniqueConstraint(const std::shared_ptr<parser::ColumnDefinition> &col) {
      UniqueInfo unique_info;

      unique_info.unique_cols_ = {col->GetColumnName()};
      unique_info.constraint_name_ = "con_unique";

      con_uniques_.push_back(unique_info);
      return *this;
    }

    /**
     * Extract check constraints
     * @param col multi-column constraint definition
     * @return builder object
     */
    Builder &ProcessCheckConstraint(const std::shared_ptr<parser::ColumnDefinition> &col) {
      auto check_cols = std::vector<std::string>();

      // TODO(Gus,Wen) more expression types need to be supported
      if (col->GetCheckExpression()->GetReturnValueType() == type::TypeId::BOOLEAN) {
        check_cols.push_back(col->GetColumnName());

        const parser::ConstantValueExpression *const_expr_elem =
            dynamic_cast<const parser::ConstantValueExpression *>(col->GetCheckExpression()->GetChild(1).get());
        type::TransientValue tmp_value = const_expr_elem->GetValue();

        CheckInfo check_info(check_cols, "con_check", col->GetCheckExpression()->GetExpressionType(),
                             std::move(tmp_value));
        con_checks_.emplace_back(std::move(check_info));
      }
      return *this;
    }

    /**
     * Build the create function plan node
     * @return plan node
     */
    std::shared_ptr<CreatePlanNode> Build() {
      return std::shared_ptr<CreatePlanNode>(new CreatePlanNode(
          std::move(children_), std::move(output_schema_), estimated_cardinality_, create_type_, std::move(table_name_),
          std::move(schema_name_), std::move(database_name_), std::move(table_schema_), index_type_, unique_index_,
          std::move(index_name_), std::move(index_attrs_), std::move(key_attrs_), has_primary_key_,
          std::move(primary_key_), std::move(foreign_keys_), std::move(con_uniques_), std::move(con_checks_),
          std::move(trigger_name_), std::move(trigger_funcnames_), std::move(trigger_args_),
          std::move(trigger_columns_), std::move(trigger_when_), trigger_type_, std::move(view_name_),
          std::move(view_query_)));
    }

   protected:
    CreateType create_type_;
    std::string table_name_;
    std::string schema_name_;
    std::string database_name_;
    std::shared_ptr<catalog::Schema> table_schema_;

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

    // CREATE VIEW
    std::string view_name_;
    std::shared_ptr<parser::SelectStatement> view_query_;
  };

  /**
   * @param children child plan nodes
   * @param output_schema Schema representing the structure of the output of this plan node
   * @param estimated_cardinality estimated cardinality of output of node
   * @param create_type type of object to create
   * @param table_name the name of the table [CREATE TABLE]
   * @param schema_name the name of the schema [CREATE TABLE, CREATE SCHEMA]
   * @param database_name the name of the database [CREATE TABLE, CREATE DATABASE]
   * @param table_schema schema of the table to create [CREATE TABLE]
   * @param index_type type of index to create [CREATE INDEX]
   * @param unique_index true if index should be unique for [CREATE INDEX]
   * @param index_name name of index to be created [CREATE INDEX]
   * @param index_attrs index attributes for [CREATE INDEX]
   * @param key_attrs key attributes for [CREATE INDEX]
   * @param has_primary_key true if index/table has primary key [CREATE INDEX/TABLE]
   * @param primary_key primary_key of table [CREATE TABLE]
   * @param foreign_keys foreign keys meta-data [CREATE TABLE]
   * @param con_uniques unique constraints [CREATE TABLE]
   * @param con_checks check constraints [CREATE TABLE]
   * @param trigger_name name of the trigger [CREATE TRIGGER]
   * @param trigger_funcnames trigger function names for [CREATE TRIGGER]
   * @param trigger_args trigger args for [CREATE TRIGGER]
   * @param trigger_columns trigger columns for [CREATE TRIGGER]
   * @param trigger_when trigger when clause for [CREATE TRIGGER]
   * @param trigger_type trigger type, i.e. information about row, timing, events, access by pg_trigger
   * @param view_name  view name for [CREATE VIEW]
   * @param view_query view query for [CREATE VIEW]
   */
  CreatePlanNode(std::vector<std::unique_ptr<AbstractPlanNode>> &&children, std::shared_ptr<OutputSchema> output_schema,
                 uint32_t estimated_cardinality, CreateType create_type, std::string table_name,
                 std::string schema_name, std::string database_name, std::shared_ptr<catalog::Schema> table_schema,
                 parser::IndexType index_type, bool unique_index, std::string index_name,
                 std::vector<std::string> &&index_attrs, std::vector<std::string> &&key_attrs, bool has_primary_key,
                 PrimaryKeyInfo primary_key, std::vector<ForeignKeyInfo> &&foreign_keys,
                 std::vector<UniqueInfo> &&con_uniques, std::vector<CheckInfo> &&con_checks, std::string trigger_name,
                 std::vector<std::string> &&trigger_funcnames, std::vector<std::string> &&trigger_args,
                 std::vector<std::string> &&trigger_columns, std::shared_ptr<parser::AbstractExpression> &&trigger_when,
                 int16_t trigger_type, std::string view_name, std::shared_ptr<parser::SelectStatement> view_query)
      : AbstractPlanNode(std::move(children), std::move(output_schema), estimated_cardinality),
        create_type_(create_type),
        table_name_(std::move(table_name)),
        schema_name_(std::move(schema_name)),
        database_name_(std::move(database_name)),
        table_schema_(std::move(table_schema)),
        index_type_(index_type),
        unique_index_(unique_index),
        index_name_(std::move(index_name)),
        index_attrs_(std::move(index_attrs)),
        key_attrs_(std::move(key_attrs)),
        has_primary_key_(has_primary_key),
        primary_key_(std::move(primary_key)),
        foreign_keys_(std::move(foreign_keys)),
        con_uniques_(std::move(con_uniques)),
        con_checks_(std::move(con_checks)),
        trigger_name_(std::move(trigger_name)),
        trigger_funcnames_(std::move(trigger_funcnames)),
        trigger_args_(std::move(trigger_args)),
        trigger_columns_(std::move(trigger_columns)),
        trigger_when_(std::move(trigger_when)),
        trigger_type_(trigger_type),
        view_name_(std::move(view_name)),
        view_query_(std::move(view_query)) {}

 public:
  CreatePlanNode() = delete;
  /**
   * @return the type of this plan node
   */
  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::CREATE; }

  /**
   * @return name of the index for [CREATE INDEX]
   */
  const std::string &GetIndexName() const { return index_name_; }
  /**
   * @return name of the table for [CREATE TABLE]
   */
  const std::string &GetTableName() const { return table_name_; }
  /**
   * @return name of the schema for [CREATE SCHEMA]
   */
  const std::string &GetSchemaName() const { return schema_name_; }

  /**
   * @return name of the database for [CREATE DATABASE]
   */
  const std::string &GetDatabaseName() const { return database_name_; }

  /**
   * @return pointer to the schema for [CREATE TABLE]
   */
  std::shared_ptr<catalog::Schema> GetTableSchema() const { return table_schema_; }

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
  const std::vector<std::string> &GetIndexAttributes() const { return index_attrs_; }

  /**
   * @return true if index/table has primary key [CREATE INDEX/TABLE]
   */
  bool HasPrimaryKey() const { return has_primary_key_; }

  /**
   * @return primary key meta-data
   */
  PrimaryKeyInfo GetPrimaryKey() const { return primary_key_; }

  /**
   * @return foreign keys meta-data
   */
  const std::vector<ForeignKeyInfo> &GetForeignKeys() const { return foreign_keys_; }

  /**
   * @return unique constraints
   */
  const std::vector<UniqueInfo> &GetUniqueConstraintss() const { return con_uniques_; }

  /**
   * @return check constraints
   */
  const std::vector<CheckInfo> &GetCheckConstrinats() const { return con_checks_; }

  /**
   * @return name of key attributes
   */
  const std::vector<std::string> &GetKeyAttrs() const { return key_attrs_; }

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

  /**
   * @return view name for [CREATE VIEW]
   */
  std::string GetViewName() const { return view_name_; }

  /**
   * @return view query for [CREATE VIEW]
   */
  std::shared_ptr<parser::SelectStatement> GetViewQuery() { return view_query_; }

  /**
   * @return the hashed value of this plan node
   */
  common::hash_t Hash() const override;

  bool operator==(const AbstractPlanNode &rhs) const override;

 private:
  // Type of object to create
  CreateType create_type_;

  // Table Name
  std::string table_name_;

  // namespace Name
  std::string schema_name_;

  // Database Name
  std::string database_name_;

  // Table Schema
  std::shared_ptr<catalog::Schema> table_schema_;

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

  // CREATE VIEW
  std::string view_name_;
  std::shared_ptr<parser::SelectStatement> view_query_;

 public:
  /**
   * Dont allow plan to be copied or moved
   */
  DISALLOW_COPY_AND_MOVE(CreatePlanNode);
};

}  // namespace terrier::plan_node
