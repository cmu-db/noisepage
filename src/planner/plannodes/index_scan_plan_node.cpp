#include "planner/plannodes/index_scan_plan_node.h"
#include "common/hash_util.h"

namespace terrier::planner {

common::hash_t IndexScanPlanNode::Hash() const {
  auto type = GetPlanNodeType();
  common::hash_t hash = common::HashUtil::Hash(&type);

  // Hash predicate
  if (GetScanPredicate() != nullptr) {
    hash = common::HashUtil::CombineHashes(hash, GetScanPredicate()->Hash());
  }

  // Hash is_for_update
  auto is_for_update = IsForUpdate();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&is_for_update));

  return common::HashUtil::CombineHashes(hash, AbstractPlanNode::Hash());
}

bool IndexScanPlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (GetPlanNodeType() != rhs.GetPlanNodeType()) return false;

  auto &rhs_plan_node = static_cast<const IndexScanPlanNode &>(rhs);

  // Predicate
  auto &pred = GetScanPredicate();
  auto &rhs_plan_node_pred = rhs_plan_node.GetScanPredicate();
  if ((pred == nullptr && rhs_plan_node_pred != nullptr) || (pred != nullptr && rhs_plan_node_pred == nullptr))
    return false;
  if (pred != nullptr && *pred != *rhs_plan_node_pred) return false;

  if (IsForUpdate() != rhs_plan_node.IsForUpdate()) return false;

  return AbstractScanPlanNode::operator==(rhs) && AbstractPlanNode::operator==(rhs);
}

nlohmann::json IndexScanPlanNode::ToJson() const {
  nlohmann::json j = AbstractScanPlanNode::ToJson();
  j["index_oid"] = index_oid_;
  return j;
}

void IndexScanPlanNode::FromJson(const nlohmann::json &j) {
  AbstractScanPlanNode::FromJson(j);
  index_oid_ = j.at("index_oid").get<catalog::index_oid_t>();
}

}  // namespace terrier::planner
