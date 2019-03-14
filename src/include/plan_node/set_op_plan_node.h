#pragma once

#include "abstract_plan_node.h"
#include "output_schema.h"

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
  SetOpPlanNode(OutputSchema output_schema, SetOpType set_op) : AbstractPlanNode(output_schema), set_op_(set_op) {}

  /**
   * @return the set operation of this node
   */
  SetOpType GetSetOp() const { return set_op_; }

  /**
   * @return the type of this plan node
   */
  inline PlanNodeType GetPlanNodeType() const { return PlanNodeType::SETOP; }

  /**
   * @return debug info
   */
  const std::string GetInfo() const { return "Set Op Plan Node"; }

  /**
   * @return a unique pointer to this plan node
   */
  std::unique_ptr<AbstractPlanNode> Copy() const {
    return std::unique_ptr<AbstractPlanNode>(new SetOpPlanNode(set_op_));
  }

 private:
  // Set Operation of this node
  SetOpType set_op_;

 private:
  DISALLOW_COPY_AND_MOVE(SetOpPlanNode);
};

}  // namespace terrier::plan_node
