#pragma once

#include <memory>
#include <string>
#include <utility>
#include "plan_node/abstract_plan_node.h"
#include "plan_node/output_schema.h"

namespace terrier::plan_node {

/**
 * Plan node for set operation:
 * INTERSECT/INTERSECT ALL/EXPECT/EXCEPT ALL
 *
 * @warning UNION (ALL) is handled differently.
 * IMPORTANT: Both children must have the same physical schema.
 */
class SetOpPlanNode : public AbstractPlanNode {
 public:
  /**
   * Instantiate a SetOpPlanNode
   * @param output_schema the output schema of this plan node
   * @param set_op the set operation of this node
   */
  SetOpPlanNode(std::shared_ptr<OutputSchema> output_schema, SetOpType set_op)
      : AbstractPlanNode(std::move(output_schema)), set_op_(set_op) {}

  /**
   * @return the set operation of this node
   */
  SetOpType GetSetOp() const { return set_op_; }

  /**
   * @return the type of this plan node
   */
  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::SETOP; }

 private:
  // Set Operation of this node
  SetOpType set_op_;

 private:
  DISALLOW_COPY_AND_MOVE(SetOpPlanNode);
};

}  // namespace terrier::plan_node
