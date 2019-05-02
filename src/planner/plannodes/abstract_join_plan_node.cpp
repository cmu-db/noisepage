#include "planner/plannodes/abstract_join_plan_node.h"

namespace terrier::planner {

bool AbstractJoinPlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (GetPlanNodeType() != rhs.GetPlanNodeType()) {
    return false;
  }

  // Check join type
  auto &other = dynamic_cast<const AbstractJoinPlanNode &>(rhs);
  if (GetLogicalJoinType() != other.GetLogicalJoinType()) {
    return false;
  }

  // Check predicate
  auto &pred = GetJoinPredicate();
  auto &other_pred = other.GetJoinPredicate();
  if ((pred == nullptr && other_pred != nullptr) || (pred != nullptr && other_pred == nullptr)) {
    return false;
  }
  if (pred != nullptr && *pred != *other_pred) {
    return false;
  }

  return true;
}

common::hash_t AbstractJoinPlanNode::Hash() const {
  auto type = GetPlanNodeType();
  common::hash_t hash = common::HashUtil::Hash(&type);

  auto join_type = GetLogicalJoinType();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&join_type));

  if (GetJoinPredicate() != nullptr) {
    hash = common::HashUtil::CombineHashes(hash, GetJoinPredicate()->Hash());
  }

  return hash;
}

}  // namespace terrier::planner
