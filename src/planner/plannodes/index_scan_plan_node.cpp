#include "planner/plannodes/index_scan_plan_node.h"
#include "common/hash_util.h"

namespace terrier::planner {

common::hash_t IndexScanPlanNode::Hash() const {
  common::hash_t hash = AbstractScanPlanNode::Hash();

  // Index Oid
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(index_oid_));

  return hash;
}

bool IndexScanPlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (!AbstractScanPlanNode::operator==(rhs)) return false;

  auto &other = static_cast<const IndexScanPlanNode &>(rhs);

  // Index Oid
  return (index_oid_ == other.index_oid_);
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
