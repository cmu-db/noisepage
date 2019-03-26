#include "plan_node/hybrid_scan_plan_node.h"
#include "common/hash_util.h"

namespace terrier::plan_node {

common::hash_t HybridScanPlanNode::Hash() const {
  auto type = GetPlanNodeType();
  common::hash_t hash = common::HashUtil::Hash(&type);

  // Hash predicate
  if (GetPredicate() != nullptr) {
    hash = common::HashUtil::CombineHashes(hash, GetPredicate()->Hash());
  }

  // Hash is_for_update
  auto is_for_update = IsForUpdate();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&is_for_update));

  // Hash HybridScanType
  auto hybrid_scan_type = GetHybridScanType();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&hybrid_scan_type));

  return common::HashUtil::CombineHashes(hash, AbstractPlanNode::Hash());
}

bool HybridScanPlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (GetPlanNodeType() != rhs.GetPlanNodeType()) return false;

  auto &other = dynamic_cast<const HybridScanPlanNode &>(rhs);

  // Predicate
  auto *pred = GetPredicate();
  auto *other_pred = other.GetPredicate();
  if ((pred == nullptr && other_pred != nullptr) || (pred != nullptr && other_pred == nullptr)) return false;
  if (pred != nullptr && *pred != *other_pred) return false;

  if (GetHybridScanType() != other.GetHybridScanType()) return false;

  if (IsForUpdate() != other.IsForUpdate()) return false;

  return AbstractPlanNode::operator==(rhs);
}

}  // namespace terrier::plan_node
