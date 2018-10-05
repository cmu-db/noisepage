//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// set_op_plan.h
//
// Identification: src/include/planner/set_op_plan.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sql/plannode/abstract_plannode.h"
#include "sql/plannode/plannode_defs.h"

namespace terrier::sql::plannode {

/**
 * @brief Plan node for set operation:
 * INTERSECT/INTERSECT ALL/EXPECT/EXCEPT ALL
 *
 * @warning UNION (ALL) is handled differently.
 * IMPORTANT: Both children must have the same physical schema.
 */
class SetOpPlanNode : public AbstractPlanNode {
 public:
  SetOpPlanNode(SetOpType set_op) : set_op_(set_op) {}

  SetOpType GetSetOpType() const { return set_op_; }

  inline PlanNodeType GetPlanNodeType() const { return PlanNodeType::SETOP; }

  const std::string GetInfo() const { return "SetOpPlanNode"; }

  std::unique_ptr<AbstractPlanNode> Copy() const {
    return std::unique_ptr<AbstractPlanNode>(new SetOpPlanNode(set_op_));
  }

 private:
  /** @brief Set Operation of this node */
  SetOpType set_op_;

 private:
  DISALLOW_COPY_AND_MOVE(SetOpPlanNode);
};

}  // namespace terrier::sql::plannode
