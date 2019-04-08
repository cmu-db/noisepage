#include "plan_node/nested_loop_join_plan_node.h"
#include <memory>

namespace terrier::plan_node {

common::hash_t NestedLoopJoinPlanNode::Hash() const {
  common::hash_t hash = AbstractJoinPlanNode::Hash();

  // Hash Predicate
  hash = common::HashUtil::CombineHashes(hash, GetJoinPredicate()->Hash());

  // Hash join type
  auto logical_join_type = GetLogicalJoinType();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&logical_join_type));

  return common::HashUtil::CombineHashes(hash, AbstractPlanNode::Hash());
}

bool NestedLoopJoinPlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (!AbstractJoinPlanNode::operator==(rhs)) {
    return false;
  }

  const auto &other = static_cast<const NestedLoopJoinPlanNode &>(rhs);

  if (GetLogicalJoinType() != other.GetLogicalJoinType()) return false;

  if (*GetJoinPredicate() != *other.GetJoinPredicate()) return false;

  return AbstractPlanNode::operator==(rhs);
}

}  // namespace terrier::plan_node
