#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "catalog/catalog_defs.h"
#include "parser/drop_statement.h"
#include "plan_node/abstract_plan_node.h"

namespace terrier::plan_node {
/**
 *  The plan node for dropping databases
 */
class DropDatabasePlanNode : public AbstractPlanNode {
 public:
  /**
   * Builder for a drop database plan node
   */
  class Builder : public AbstractPlanNode::Builder<Builder> {
   public:
    Builder() = default;

    /**
     * Don't allow builder to be copied or moved
     */
    DISALLOW_COPY_AND_MOVE(Builder);

    /**
     * @param database_name the name of the database
     * @return builder object
     */
    Builder &SetDatabaseName(std::string database_name) {
      database_name_ = std::move(database_name);
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
      if (drop_stmt->GetDropType() == parser::DropStatement::DropType::kDatabase) {
        database_name_ = drop_stmt->GetDatabaseName();
        if_exists_ = drop_stmt->IsIfExists();
      }
      return *this;
    }

    /**
     * Build the drop database plan node
     * @return plan node
     */
    std::unique_ptr<DropDatabasePlanNode> Build() {
      return std::unique_ptr<DropDatabasePlanNode>(new DropDatabasePlanNode(
          std::move(children_), std::move(output_schema_), std::move(database_name_), if_exists_));
    }

   protected:
    /**
     * Database Name
     */
    std::string database_name_;

    /**
     * Whether "IF EXISTS" was used
     */
    bool if_exists_;
  };

 private:
  /**
   * @param children child plan nodes
   * @param output_schema Schema representing the structure of the output of this plan node
   * @param drop_type type of object to drop
   * @param database_name the name of the database
   */
  DropDatabasePlanNode(std::vector<std::unique_ptr<AbstractPlanNode>> &&children,
                       std::shared_ptr<OutputSchema> output_schema, std::string database_name, bool if_exists)
      : AbstractPlanNode(std::move(children), std::move(output_schema)),
        database_name_(std::move(database_name)),
        if_exists_(if_exists) {}

 public:
  DropDatabasePlanNode() = delete;

  /**
   * @return the type of this plan node
   */
  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::DROP_DATABASE; }

  /**
   * @return database name
   */
  std::string GetDatabaseName() const { return database_name_; }

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
  /**
   * Database Name
   */
  std::string database_name_;

  /**
   * Whether "IF EXISTS" was used
   */
  bool if_exists_;

 public:
  /**
   * Don't allow plan to be copied or moved
   */
  DISALLOW_COPY_AND_MOVE(DropDatabasePlanNode);
};

}  // namespace terrier::plan_node
