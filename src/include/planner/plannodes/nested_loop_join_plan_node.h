#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "planner/plannodes/abstract_join_plan_node.h"
#include "planner/plannodes/plan_visitor.h"

namespace noisepage::planner {

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
    std::unique_ptr<NestedLoopJoinPlanNode> Build() {
      return std::unique_ptr<NestedLoopJoinPlanNode>(
          new NestedLoopJoinPlanNode(std::move(children_), std::move(output_schema_), join_type_, join_predicate_));
    }
  };

 private:
  /**
   * @param children child plan nodes, first child is inner loop, second child is outer loop
   * @param output_schema Schema representing the structure of the output of this plan node
   * @param join_type logical join type
   * @param predicate join predicate
   */
  NestedLoopJoinPlanNode(std::vector<std::unique_ptr<AbstractPlanNode>> &&children,
                         std::unique_ptr<OutputSchema> output_schema, LogicalJoinType join_type,
                         common::ManagedPointer<parser::AbstractExpression> predicate)
      : AbstractJoinPlanNode(std::move(children), std::move(output_schema), join_type, predicate) {}

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

  void Accept(common::ManagedPointer<PlanVisitor> v) const override { v->Visit(this); }

  nlohmann::json ToJson() const override;
  std::vector<std::unique_ptr<parser::AbstractExpression>> FromJson(const nlohmann::json &j) override;
};

DEFINE_JSON_HEADER_DECLARATIONS(NestedLoopJoinPlanNode);

}  // namespace noisepage::planner
