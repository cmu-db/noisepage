#pragma once

#include <memory>
#include <string>
#include <utility>
#include "plan_node/abstract_join_plan_node.h"

namespace terrier::plan_node {

/**
 * Plan node for nested loop joins
 */
class NestedLoopJoinPlanNode : public AbstractJoinPlanNode {
 public:
  /**
   * @param output_schema Schema representing the structure of the output of this plan node
   * @param join_type logical join type
   * @param predicate join predicate
   */
  NestedLoopJoinPlanNode(std::shared_ptr<OutputSchema> output_schema, LogicalJoinType join_type,
                         parser::AbstractExpression *predicate)
      : AbstractJoinPlanNode(std::move(output_schema), join_type, predicate) {}

  /**
   * @return the type of this plan node
   */
  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::NESTLOOP; }

  /**
   * @return the hashed value of this plan node
   */
  common::hash_t Hash() const override;

  bool operator==(const AbstractPlanNode &rhs) const override;
  bool operator!=(const AbstractPlanNode &rhs) const override { return !(*this == rhs); }

 public:
  DISALLOW_COPY_AND_MOVE(NestedLoopJoinPlanNode);
};

}  // namespace terrier::plan_node
