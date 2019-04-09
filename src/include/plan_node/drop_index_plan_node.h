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
 *  The plan node for dropping indexes
 */
class DropIndexPlanNode : public AbstractPlanNode {
 public:
  /**
   * Builder for a drop index plan node
   */
  class Builder : public AbstractPlanNode::Builder<Builder> {
   public:
    Builder() = default;

    /**
     * Don't allow builder to be copied or moved
     */
    DISALLOW_COPY_AND_MOVE(Builder);

    /**
     * @param index_name the name of the index
     * @return builder object
     */
    Builder &SetIndexName(std::string index_name) {
      index_name_ = std::move(index_name);
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
      if (drop_stmt->GetDropType() == parser::DropStatement::DropType::kIndex) {
        index_name_ = drop_stmt->GetIndexName();
        if_exists_ = drop_stmt->IsIfExists();
      }
      return *this;
    }

    /**
     * Build the drop index plan node
     * @return plan node
     */
    std::unique_ptr<DropIndexPlanNode> Build() {
      return std::unique_ptr<DropIndexPlanNode>(
          new DropIndexPlanNode(std::move(children_), std::move(output_schema_), std::move(index_name_), if_exists_));
    }

   protected:
    /**
     * Index name
     */
    std::string index_name_;

    /**
     * Whether "IF EXISTS" was used
     */
    bool if_exists_;
  };

 private:
  /**
   * @param children child plan nodes
   * @param output_schema Schema representing the structure of the output of this plan node
   * @param index_name the name of the index
   */
  DropIndexPlanNode(std::vector<std::unique_ptr<AbstractPlanNode>> &&children,
                    std::shared_ptr<OutputSchema> output_schema, std::string index_name, bool if_exists)
      : AbstractPlanNode(std::move(children), std::move(output_schema)),
        index_name_(std::move(index_name)),
        if_exists_(if_exists) {}

 public:
  DropIndexPlanNode() = delete;

  /**
   * @return the type of this plan node
   */
  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::DROP_INDEX; }

  /**
   * @return index name
   */
  std::string GetIndexName() const { return index_name_; }

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
   * Index name
   */
  std::string index_name_;

  /**
   * Whether "IF EXISTS" was used
   */
  bool if_exists_;

 public:
  /**
   * Don't allow plan to be copied or moved
   */
  DISALLOW_COPY_AND_MOVE(DropIndexPlanNode);
};

}  // namespace terrier::plan_node
