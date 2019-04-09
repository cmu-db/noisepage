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
 *  The plan node for dropping triggers
 */
class DropTriggerPlanNode : public AbstractPlanNode {
 public:
  /**
   * Builder for a drop trigger plan node
   */
  class Builder : public AbstractPlanNode::Builder<Builder> {
   public:
    Builder() = default;

    /**
     * Don't allow builder to be copied or moved
     */
    DISALLOW_COPY_AND_MOVE(Builder);

    /**
     * @param trigger_name the name of the trigger
     * @return builder object
     */
    Builder &SetTriggerName(std::string trigger_name) {
      trigger_name_ = std::move(trigger_name);
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
      if (drop_stmt->GetDropType() == parser::DropStatement::DropType::kTrigger) {
        trigger_name_ = drop_stmt->GetTriggerName();
        if_exists_ = drop_stmt->IsIfExists();
      }
      return *this;
    }

    /**
     * Build the drop trigger plan node
     * @return plan node
     */
    std::unique_ptr<DropTriggerPlanNode> Build() {
      return std::unique_ptr<DropTriggerPlanNode>(new DropTriggerPlanNode(
          std::move(children_), std::move(output_schema_), std::move(trigger_name_), if_exists_));
    }

   protected:
    /**
     * Trigger name
     */
    std::string trigger_name_;

    /**
     * Whether "IF EXISTS" was used
     */
    bool if_exists_;
  };

 private:
  /**
   * @param children child plan nodes
   * @param output_trigger Trigger representing the structure of the output of this plan node
   * @param trigger_name the name of the trigger
   */
  DropTriggerPlanNode(std::vector<std::unique_ptr<AbstractPlanNode>> &&children,
                      std::shared_ptr<OutputSchema> output_trigger, std::string trigger_name, bool if_exists)
      : AbstractPlanNode(std::move(children), std::move(output_trigger)),
        trigger_name_(std::move(trigger_name)),
        if_exists_(if_exists) {}

 public:
  DropTriggerPlanNode() = delete;

  /**
   * @return the type of this plan node
   */
  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::DROP_TRIGGER; }

  /**
   * @return trigger name
   */
  std::string GetTriggerName() const { return trigger_name_; }

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
   * Trigger name
   */
  std::string trigger_name_;

  /**
   * Whether "IF EXISTS" was used
   */
  bool if_exists_;

 public:
  /**
   * Don't allow plan to be copied or moved
   */
  DISALLOW_COPY_AND_MOVE(DropTriggerPlanNode);
};

}  // namespace terrier::plan_node
