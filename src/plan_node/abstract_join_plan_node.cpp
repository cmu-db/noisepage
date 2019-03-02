#include "plan_node/abstract_join_plan_node.h"

namespace terrier::plan_node {

bool AbstractJoinPlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (GetPlanNodeType() != rhs.GetPlanNodeType()) {
    return false;
  }

  // Check join type
  auto &other = static_cast<const AbstractJoinPlanNode &>(rhs);
  if (GetLogicalJoinType() != other.GetLogicalJoinType()) {
    return false;
  }

  // Check predicate
  auto *pred = GetPredicate();
  auto *other_pred = other.GetPredicate();
  if ((pred == nullptr && other_pred != nullptr) ||
      (pred != nullptr && other_pred == nullptr)) {
    return false;
  }
  if (pred != nullptr && *pred != *other_pred) {
    return false;
  }

  //TODO(Gus,Wen): Compare output schema

  return true;
}

common::hash_t AbstractJoinPlanNode::Hash() const {
  auto type = GetPlanNodeType();
  common::hash_t hash = common::HashUtil::Hash(&type);

  auto join_type = GetLogicalJoinType();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&join_type));

  if (GetPredicate() != nullptr) {
    hash = common::HashUtil::CombineHashes(hash, GetPredicate()->Hash());
  }

  return hash;
}

}  // namespace terrier::plan_node
