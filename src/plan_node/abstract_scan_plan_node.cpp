#include "plan_node/abstract_scan_plan_node.h"

namespace terrier::plan_node {

bool AbstractScanPlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (GetPlanNodeType() != rhs.GetPlanNodeType()) {
    return false;
  }

  // Check predicate
  auto &other = dynamic_cast<const AbstractScanPlanNode &>(rhs);

  auto &pred = GetScanPredicate();
  auto &other_pred = other.GetScanPredicate();
  if ((pred == nullptr && other_pred != nullptr) || (pred != nullptr && other_pred == nullptr)) {
    return false;
  }
  if (pred != nullptr && *pred != *other_pred) {
    return false;
  }

  return IsForUpdate() == other.IsForUpdate() && IsParallel() == other.IsParallel() &&
         GetDatabaseOid() == other.GetDatabaseOid();
}

common::hash_t AbstractScanPlanNode::Hash() const {
  auto type = GetPlanNodeType();
  common::hash_t hash = common::HashUtil::Hash(&type);

  // Hash predicate
  if (GetScanPredicate() != nullptr) {
    hash = common::HashUtil::CombineHashes(hash, GetScanPredicate()->Hash());
  }

  // Hash update flag
  auto is_for_update = IsForUpdate();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&is_for_update));

  // Hash parallel flag
  auto is_parallel = IsParallel();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&is_parallel));

  // Hash database oid
  auto database_oid = GetDatabaseOid();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&database_oid));

  return hash;
}

}  // namespace terrier::plan_node
