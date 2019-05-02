#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "planner/plannodes/abstract_join_plan_node.h"

namespace terrier::planner {

/**
 * Plan node for nested loop joins
 */
class NestedLoopJoinPlanNode : public AbstractJoinPlanNode {
 public:
  /**
   * Builder for nested loop join plan node
   */
  class Builder : public AbstractJoinPlanNode::Builder<Builder> {
   public:
    Builder() = default;

    /**
     * Don't allow builder to be copied or moved
     */
    DISALLOW_COPY_AND_MOVE(Builder);

    /**
     * Build the nested loop join plan node
     * @return plan node
     */
    std::shared_ptr<NestedLoopJoinPlanNode> Build() {
      return std::shared_ptr<NestedLoopJoinPlanNode>(new NestedLoopJoinPlanNode(
          std::move(children_), std::move(output_schema_), join_type_, std::move(join_predicate_)));
    }
  };

 private:
  /**
   * @param children child plan nodes
   * @param output_schema Schema representing the structure of the output of this plan node
   * @param join_type logical join type
   * @param predicate join predicate
   */
  NestedLoopJoinPlanNode(std::vector<std::shared_ptr<AbstractPlanNode>> &&children,
                         std::shared_ptr<OutputSchema> output_schema, LogicalJoinType join_type,
                         std::shared_ptr<parser::AbstractExpression> predicate)
      : AbstractJoinPlanNode(std::move(children), std::move(output_schema), join_type, std::move(predicate)) {}

 public:
  /**
   * Default constructor used for deserialization
   */
  NestedLoopJoinPlanNode() = default;

  DISALLOW_COPY_AND_MOVE(NestedLoopJoinPlanNode)

  /**
   * @return the type of this plan node
   */
  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::NESTLOOP; }

  /**
   * @return the NestedLooped value of this plan node
   */
  common::hash_t Hash() const override;

  bool operator==(const AbstractPlanNode &rhs) const override;

  nlohmann::json ToJson() const override;
  void FromJson(const nlohmann::json &j) override;
};

DEFINE_JSON_DECLARATIONS(NestedLoopJoinPlanNode);

}  // namespace terrier::planner
