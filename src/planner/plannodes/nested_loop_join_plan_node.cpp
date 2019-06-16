#include "planner/plannodes/nested_loop_join_plan_node.h"
#include <memory>

namespace terrier::planner {

common::hash_t NestedLoopJoinPlanNode::Hash() const {
  common::hash_t hash = AbstractJoinPlanNode::Hash();

  // There is nothing else for us to do here!

  return hash;
}

bool NestedLoopJoinPlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (GetPlanNodeType() != rhs.GetPlanNodeType()) return false;

  auto &other = dynamic_cast<const NestedLoopJoinPlanNode &>(rhs);

  // There is nothing else for us to do here! Go home! You're drunk!

  return AbstractJoinPlanNode::operator==(other);
}

nlohmann::json NestedLoopJoinPlanNode::ToJson() const { return AbstractJoinPlanNode::ToJson(); }

void NestedLoopJoinPlanNode::FromJson(const nlohmann::json &j) { AbstractJoinPlanNode::FromJson(j); }

}  // namespace terrier::planner
