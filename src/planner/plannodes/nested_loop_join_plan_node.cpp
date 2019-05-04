#include "planner/plannodes/nested_loop_join_plan_node.h"
#include <memory>

namespace terrier::planner {

common::hash_t NestedLoopJoinPlanNode::Hash() const { return AbstractJoinPlanNode::Hash(); }

bool NestedLoopJoinPlanNode::operator==(const AbstractPlanNode &rhs) const {
  return AbstractJoinPlanNode::operator==(rhs);
}

nlohmann::json NestedLoopJoinPlanNode::ToJson() const { return AbstractJoinPlanNode::ToJson(); }

void NestedLoopJoinPlanNode::FromJson(const nlohmann::json &j) { AbstractJoinPlanNode::FromJson(j); }

}  // namespace terrier::planner
