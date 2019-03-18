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

  auto &rhs_plan_node = static_cast<const HybridScanPlanNode &>(rhs);

  // Predicate
  auto *pred = GetPredicate();
  auto *rhs_plan_node_pred = rhs_plan_node.GetPredicate();
  if ((pred == nullptr && rhs_plan_node_pred != nullptr) || (pred != nullptr && rhs_plan_node_pred == nullptr))
    return false;
  if (pred != nullptr && *pred != *rhs_plan_node_pred) return false;

  if (*GetOutputSchema() != *rhs_plan_node.GetOutputSchema()) return false;

  if (GetHybridScanType() != rhs_plan_node.GetHybridScanType()) return false;

  if (IsForUpdate() != rhs_plan_node.IsForUpdate()) return false;

  return AbstractPlanNode::operator==(rhs);
}

}  // namespace terrier::plan_node
