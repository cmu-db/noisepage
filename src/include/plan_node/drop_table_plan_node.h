#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "parser/drop_statement.h"
#include "plan_node/abstract_plan_node.h"
#include "transaction/transaction_context.h"

namespace terrier::plan_node {
/**
 *  The plan node for dropping tables
 */
class DropTablePlanNode : public AbstractPlanNode {
 public:
  /**
   * Builder for a drop table plan node
   */
  class Builder : public AbstractPlanNode::Builder<Builder> {
   public:
    Builder() = default;

    /**
     * Don't allow builder to be copied or moved
     */
    DISALLOW_COPY_AND_MOVE(Builder);

    /**
     * @param table_name the name of the table
     * @return builder object
     */
    Builder &SetTableName(std::string table_name) {
      table_name_ = std::move(table_name);
      return *this;
    }

    /**
     * @param if_exists true if "IF EXISTS" was used
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
      if (drop_stmt->GetDropType() == parser::DropStatement::DropType::kTable) {
        table_name_ = drop_stmt->GetTableName();
        if_exists_ = drop_stmt->IsIfExists();
      }
      return *this;
    }

    /**
     * Build the drop table plan node
     * @return plan node
     */
    std::unique_ptr<DropTablePlanNode> Build() {
      return std::unique_ptr<DropTablePlanNode>(
          new DropTablePlanNode(std::move(children_), std::move(output_schema_), std::move(table_name_), if_exists_));
    }

   protected:
    /**
     * Table name
     */
    std::string table_name_;

    /**
     * Whether "IF EXISTS" was used
     */
    bool if_exists_;
  };

 private:
  /**
   * @param children child plan nodes
   * @param output_schema Schema representing the structure of the output of this plan node
   * @param table_name the name of the table
   */
  DropTablePlanNode(std::vector<std::unique_ptr<AbstractPlanNode>> &&children,
                    std::shared_ptr<OutputSchema> output_schema, std::string table_name, bool if_exists)
      : AbstractPlanNode(std::move(children), std::move(output_schema)),
        table_name_(std::move(table_name)),
        if_exists_(if_exists) {}

 public:
  DropTablePlanNode() = delete;

  /**
   * @return the type of this plan node
   */
  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::DROP_TABLE; }

  /**
   * @return table name
   */
  std::string GetTableName() const { return table_name_; }

  /**
   * @return true if "IF EXISTS" was used
   */
  bool IsIfExists() const { return if_exists_; }

  /**
   * @return the hashed value of this plan node
   */
  common::hash_t Hash() const override;

  bool operator==(const AbstractPlanNode &rhs) const override;

 private:
  /**
   * Table name
   */
  std::string table_name_;

  /**
   * Whether "IF EXISTS" was used
   */
  bool if_exists_;

 public:
  /**
   * Don't allow plan to be copied or moved
   */
  DISALLOW_COPY_AND_MOVE(DropTablePlanNode);
};

}  // namespace terrier::plan_node
