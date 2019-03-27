#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "parser/drop_statement.h"
#include "plan_node/abstract_plan_node.h"
#include "transaction/transaction_context.h"

namespace terrier {
namespace catalog {
class Schema;
}

namespace plan_node {
/**
 *  The plan node for DROP
 */
class DropPlanNode : public AbstractPlanNode {
 protected:
  /**
   * Builder for a drop plan node
   */
  class Builder : public AbstractPlanNode::Builder<Builder> {
   public:
    /**
     * Dont allow builder to be copied or moved
     */
    DISALLOW_COPY_AND_MOVE(Builder);

    /**
     * @param drop_type type of object to drop
     * @return builder object
     */
    Builder &SetDropType(DropType drop_type) {
      drop_type_ = drop_type;
      return *this;
    }

    /**
     * @param table_name the name of the table [DROP TABLE]
     * @return builder object
     */
    Builder &SetTableName(std::string table_name) {
      table_name_ = std::move(table_name);
      return *this;
    }

    /**
     * @param database_name the name of the database [DROP TABLE, DROP DATABASE
     * @return builder object
     */
    Builder &SetDatabaseName(std::string database_name) {
      database_name_ = std::move(database_name);
      return *this;
    }

    /**
     * @param schema_name the name of the schema [DROP SCHEMA]
     * @return builder object
     */
    Builder &SetSchemaName(std::string schema_name) {
      schema_name_ = std::move(schema_name);
      return *this;
    }

    /**
     * @param trigger_name the name of the trigger [DROP TRIGGER]
     * @return builder object
     */
    Builder &SetTriggerName(std::string trigger_name) {
      trigger_name_ = std::move(trigger_name);
      return *this;
    }

    /**
     * @param index_name the name of the index for [DROP INDEX]
     * @return builder object
     */
    Builder &SetIndexName(std::string index_name) {
      index_name_ = std::move(index_name);
      return *this;
    }

    /**
     * @param if_exists true if "IF EXISTS" was used for [DROP DATABASE, DROP SCHEMA]
     * @return builder object
     */
    Builder &SetIfExist(bool if_exists) {
      if_exists_ = if_exists;
      return *this;
    }

    /**
     * @param drop_stmt the SQL DROP statement
     * @return builder object
     */
    Builder &SetFromDropStatement(parser::DropStatement *drop_stmt) {
      switch (drop_stmt->GetDropType()) {
        case parser::DropStatement::DropType::kDatabase:
          database_name_ = drop_stmt->GetDatabaseName();
          if_exists_ = drop_stmt->IsIfExists();
          drop_type_ = DropType::DB;
          break;
        case parser::DropStatement::DropType::kSchema:
          database_name_ = drop_stmt->GetDatabaseName();
          schema_name_ = drop_stmt->GetSchemaName();
          if_exists_ = drop_stmt->IsIfExists();
          drop_type_ = DropType::SCHEMA;
          break;
        case parser::DropStatement::DropType::kTable:
          database_name_ = drop_stmt->GetDatabaseName();
          schema_name_ = drop_stmt->GetSchemaName();
          table_name_ = drop_stmt->GetTableName();
          if_exists_ = drop_stmt->IsIfExists();
          drop_type_ = DropType::TABLE;
          break;
        case parser::DropStatement::DropType::kTrigger:
          database_name_ = drop_stmt->GetDatabaseName();
          schema_name_ = drop_stmt->GetSchemaName();
          table_name_ = drop_stmt->GetTableName();
          trigger_name_ = drop_stmt->GetTriggerName();
          drop_type_ = DropType::TRIGGER;
          break;
        case parser::DropStatement::DropType::kIndex:
          database_name_ = drop_stmt->GetDatabaseName();
          schema_name_ = drop_stmt->GetSchemaName();
          index_name_ = drop_stmt->GetIndexName();
          drop_type_ = DropType::INDEX;
          break;
        default:
          break;
      }
      return *this;
    }

    /**
     * Build the drop function plan node
     * @return plan node
     */
    std::shared_ptr<DropPlanNode> Build() {
      return std::shared_ptr<DropPlanNode>(
          new DropPlanNode(std::move(children_), std::move(output_schema_), estimated_cardinality_, drop_type_,
                           std::move(table_name_), std::move(database_name_), std::move(schema_name_),
                           std::move(trigger_name_), std::move(index_name_), if_exists_));
    }

   protected:
    DropType drop_type_ = DropType::TABLE;
    std::string table_name_;
    std::string database_name_;
    std::string schema_name_;
    std::string trigger_name_;
    std::string index_name_;
    bool if_exists_;
  };

  /**
   * @param children child plan nodes
   * @param output_schema Schema representing the structure of the output of this plan node
   * @param estimated_cardinality estimated cardinality of output of node
   * @param drop_type type of object to drop
   * @param table_name the name of the table [DROP TABLE]
   * @param database_name the name of the database [DROP TABLE, DROP DATABASE]
   * @param schema_name the name of the schema [DROP SCHEMA]
   * @param trigger_name the name of the trigger [DROP TRIGGER]
   * @param index_name the name of the index [DROP INDEX]
   */
  DropPlanNode(std::vector<std::unique_ptr<AbstractPlanNode>> &&children, std::shared_ptr<OutputSchema> output_schema,
               uint32_t estimated_cardinality, DropType drop_type, std::string table_name, std::string database_name,
               std::string schema_name, std::string trigger_name, std::string index_name, bool if_exists)
      : AbstractPlanNode(std::move(children), std::move(output_schema), estimated_cardinality),
        drop_type_(drop_type),
        table_name_(std::move(table_name)),
        database_name_(std::move(database_name)),
        schema_name_(std::move(schema_name)),
        trigger_name_(std::move(trigger_name)),
        index_name_(std::move(index_name)),
        if_exists_(if_exists) {}

 public:
  DropPlanNode() = delete;

  /**
   * @return the type of this plan node
   */
  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::DROP; }

  /**
   * @return database name [DROP DATABASE]
   */
  std::string GetDatabaseName() const { return database_name_; }

  /**
   * @return table name for [DROP TABLE/TRIGGER]
   */
  std::string GetTableName() const { return table_name_; }

  /**
   * @return schema name for [DROP SCHEMA]
   */
  std::string GetSchemaName() const { return schema_name_; }

  /**
   * @return trigger name for [DROP TRIGGER]
   */
  std::string GetTriggerName() const { return trigger_name_; }

  /**
   * @return index name for [DROP INDEX]
   */
  std::string GetIndexName() const { return index_name_; }

  /**
   * @return drop type
   */
  DropType GetDropType() const { return drop_type_; }

  /**
   * @return true if "IF EXISTS" was used for [DROP DATABASE, DROP SCHEMA]
   */
  bool IsIfExists() const { return if_exists_; }

  /**
   * @return the hashed value of this plan node
   */
  common::hash_t Hash() const override;

  bool operator==(const AbstractPlanNode &rhs) const override;

 private:
  // Type of object to drop
  DropType drop_type_ = DropType::TABLE;

  // Target Table
  std::string table_name_;

  // Database Name
  std::string database_name_;

  // Schema Name
  std::string schema_name_;

  // Trigger Name
  std::string trigger_name_;

  // Index Name
  std::string index_name_;

  // Whether "IF EXISTS" was used for [DROP DATABASE, DROP SCHEMA]
  bool if_exists_;

 public:
  /**
   * Dont allow plan to be copied or moved
   */
  DISALLOW_COPY_AND_MOVE(DropPlanNode);
};

}  // namespace plan_node
}  // namespace terrier
