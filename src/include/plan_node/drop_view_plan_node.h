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
 *  The plan node for dropping views
 */
class DropViewPlanNode : public AbstractPlanNode {
 public:
  /**
   * Builder for a drop view plan node
   */
  class Builder : public AbstractPlanNode::Builder<Builder> {
   public:
    Builder() = default;

    /**
     * Don't allow builder to be copied or moved
     */
    DISALLOW_COPY_AND_MOVE(Builder);

    /**
     * @param view_name the name of the view
     * @return builder object
     */
    Builder &SetViewName(std::string view_name) {
      view_name_ = std::move(view_name);
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
      // TODO(Gus,Wen) Need to implement
      /* if (drop_stmt->GetDropType() == parser::DropStatement::DropType::kView) {

      }*/
      return *this;
    }

    /**
     * Build the drop view plan node
     * @return plan node
     */
    std::unique_ptr<DropViewPlanNode> Build() {
      return std::unique_ptr<DropViewPlanNode>(
          new DropViewPlanNode(std::move(children_), std::move(output_schema_), std::move(view_name_), if_exists_));
    }

   protected:
    /**
     * View name
     */
    std::string view_name_;

    /**
     * Whether "IF EXISTS" was used
     */
    bool if_exists_;
  };

 private:
  /**
   * @param children child plan nodes
   * @param output_view View representing the structure of the output of this plan node
   * @param view_name the name of the view
   */
  DropViewPlanNode(std::vector<std::unique_ptr<AbstractPlanNode>> &&children, std::shared_ptr<OutputSchema> output_view,
                   std::string view_name, bool if_exists)
      : AbstractPlanNode(std::move(children), std::move(output_view)),
        view_name_(std::move(view_name)),
        if_exists_(if_exists) {}

 public:
  DropViewPlanNode() = delete;

  /**
   * @return the type of this plan node
   */
  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::DROP_VIEW; }

  /**
   * @return view name
   */
  std::string GetViewName() const { return view_name_; }

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
   * View name
   */
  std::string view_name_;

  /**
   * Whether "IF EXISTS" was used
   */
  bool if_exists_;

 public:
  /**
   * Don't allow plan to be copied or moved
   */
  DISALLOW_COPY_AND_MOVE(DropViewPlanNode);
};

}  // namespace terrier::plan_node
