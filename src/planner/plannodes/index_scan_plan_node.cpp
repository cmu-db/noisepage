#include "planner/plannodes/index_scan_plan_node.h"
#include "common/hash_util.h"

namespace terrier::planner {

common::hash_t IndexScanPlanNode::Hash() const {
  common::hash_t hash = AbstractScanPlanNode::Hash();

  // Hash index oid
  auto index_oid = GetIndexOid();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&index_oid));

  return hash;
}

bool IndexScanPlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (!AbstractScanPlanNode::operator==(rhs)) return false;

  auto &rhs_plan_node = static_cast<const IndexScanPlanNode &>(rhs);

  return GetIndexOid() == rhs_plan_node.GetIndexOid();
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
