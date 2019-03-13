#include "plan_node/index_scan_plan_node.h"
#include "common/hash_util.h"

namespace terrier::plan_node {

common::hash_t IndexScanPlanNode::Hash() const {
  auto type = GetPlanNodeType();
  common::hash_t hash = common::HashUtil::Hash(&type);

  // Hash predicate
  if (GetPredicate() != nullptr) {
    hash = common::HashUtil::CombineHashes(hash, GetPredicate()->Hash());
  }

  // TODO(Gus,Wen): Hash output_schema

  // Hash is_for_update
  auto is_for_update = IsForUpdate();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&is_for_update));

  return common::HashUtil::CombineHashes(hash, AbstractPlanNode::Hash());
}

bool IndexScanPlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (GetPlanNodeType() != rhs.GetPlanNodeType()) return false;

  auto &rhs_plan_node = static_cast<const IndexScanPlanNode &>(rhs);

  // Predicate
  auto *pred = GetPredicate();
  auto *rhs_plan_node_pred = rhs_plan_node.GetPredicate();
  if ((pred == nullptr && rhs_plan_node_pred != nullptr) || (pred != nullptr && rhs_plan_node_pred == nullptr))
    return false;
  if (pred && *pred != *rhs_plan_node_pred) return false;

  // TODO(Gus,Wen): Include output schema equality

  if (IsForUpdate() != rhs_plan_node.IsForUpdate()) return false;

  return AbstractPlanNode::operator==(rhs);
}

}  // namespace terrier::plan_node
