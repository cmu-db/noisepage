#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "catalog/schema.h"
#include "planner/plannodes/abstract_plan_node.h"
#include "planner/plannodes/plan_visitor.h"

namespace terrier::planner {

/**
 * Plan node for a limit operator
 */
class CteScanPlanNode : public AbstractPlanNode {
 public:
  /**
   * Builder for cte scan plan node
   */
  class Builder : public AbstractPlanNode::Builder<Builder> {
   public:
    Builder() = default;

    /**
     * Don't allow builder to be copied or moved
     */
    DISALLOW_COPY_AND_MOVE(Builder);

    /**
     * @param is_leader bool indicating leader or not
     * @return builder object
     */
    Builder &SetLeader(bool is_leader) {
      is_leader_ = is_leader;
      return *this;
    }

    /**
     * @param output_schema output schema for plan node
     * @return builder object
     */
    Builder &SetTableOutputSchema(std::unique_ptr<OutputSchema> table_output_schema) {
      table_output_schema_ = std::move(table_output_schema);
      return *this;
    }

    /**
     * Build the limit plan node
     * @return plan node
     */
    std::unique_ptr<CteScanPlanNode> Build() {
      return std::unique_ptr<CteScanPlanNode>(
          new CteScanPlanNode(std::move(children_), std::move(output_schema_), is_leader_, std::move(table_output_schema_)));
    }

   protected:
   private:
    bool is_leader_ = false;
    std::unique_ptr<OutputSchema> table_output_schema_;
  };

 private:
  /**
   * @param children child plan nodes
   * @param output_schema Schema representing the structure of the output of this plan node
   */
  CteScanPlanNode(std::vector<std::unique_ptr<AbstractPlanNode>> &&children, std::unique_ptr<OutputSchema> output_schema,
      bool is_leader, std::unique_ptr<OutputSchema> table_output_schema)
      : AbstractPlanNode(std::move(children), std::move(output_schema)), is_leader_(is_leader),
      table_output_schema_(std::move(table_output_schema)){}

 public:
  /**
   * Constructor used for JSON serialization
   */
  CteScanPlanNode() = default;

  DISALLOW_COPY_AND_MOVE(CteScanPlanNode)

  /**
   * @return the type of this plan node
   */
  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::CTESCAN; }

  /**
   * @return the hashed value of this plan node
   */
  common::hash_t Hash() const override;

  bool operator==(const AbstractPlanNode &rhs) const override;

  void Accept(common::ManagedPointer<PlanVisitor> v) const override { v->Visit(this); }

  bool IsLeader() const {return  is_leader_;}

  void SetLeader() {is_leader_ = true;}

  /**
   * @return table output schema for the node. The output schema contains information on columns of the output of the plan
   * node operator
   */
  common::ManagedPointer<OutputSchema> GetTableOutputSchema() const { return common::ManagedPointer(table_output_schema_); }

  nlohmann::json ToJson() const override;
  std::vector<std::unique_ptr<parser::AbstractExpression>> FromJson(const nlohmann::json &j) override;

  //===--------------------------------------------------------------------===//
  // Update schema
  //===--------------------------------------------------------------------===//

  /**
   * output schema for the node. The output schema contains information on columns of the output of the plan
   * node operator
   */
  void SetTableOutputSchema(std::unique_ptr<OutputSchema> schema) {
    // TODO(preetang): Test for memory leak
    table_output_schema_ = std::move(schema);
  }

 private:
  bool is_leader_;
  std::unique_ptr<OutputSchema> table_output_schema_;
};

DEFINE_JSON_DECLARATIONS(CteScanPlanNode);

}  // namespace terrier::planner
