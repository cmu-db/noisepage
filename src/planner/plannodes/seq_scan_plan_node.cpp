#include "planner/plannodes/seq_scan_plan_node.h"
#include "common/hash_util.h"

namespace terrier::planner {

common::hash_t SeqScanPlanNode::Hash() const {
  auto type = GetPlanNodeType();
  common::hash_t hash = common::HashUtil::Hash(&type);

  // Hash predicate
  if (GetScanPredicate() != nullptr) {
    hash = common::HashUtil::CombineHashes(hash, GetScanPredicate()->Hash());
  }

  hash = common::HashUtil::CombineHashes(hash, GetOutputSchema()->Hash());

  // Hash is parallel
  auto is_parallel = IsParallel();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&is_parallel));

  // Hash is_for_update
  auto is_for_update = IsForUpdate();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&is_for_update));

  return common::HashUtil::CombineHashes(hash, AbstractPlanNode::Hash());
}

bool SeqScanPlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (GetPlanNodeType() != rhs.GetPlanNodeType()) return false;

  auto &rhs_plan_node = static_cast<const SeqScanPlanNode &>(rhs);

  // Predicate
  auto &pred = GetScanPredicate();
  auto &rhs_plan_node_pred = rhs_plan_node.GetScanPredicate();
  if ((pred == nullptr && rhs_plan_node_pred != nullptr) || (pred != nullptr && rhs_plan_node_pred == nullptr))
    return false;
  if (pred != nullptr && *pred != *rhs_plan_node_pred) return false;

  if (IsParallel() != rhs_plan_node.IsParallel()) return false;

  if (IsForUpdate() != rhs_plan_node.IsForUpdate()) return false;

  return AbstractScanPlanNode::operator==(rhs) && AbstractPlanNode::operator==(rhs);
}

}  // namespace terrier::planner
