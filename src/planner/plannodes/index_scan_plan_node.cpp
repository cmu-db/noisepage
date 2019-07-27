#include <vector>

#include "common/hash_util.h"
#include "planner/plannodes/index_scan_plan_node.h"

namespace terrier::planner {

common::hash_t IndexScanPlanNode::Hash() const {
  common::hash_t hash = AbstractScanPlanNode::Hash();

  // Index Oid
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(index_oid_));

  hash = common::HashUtil::CombineHashInRange(hash, column_ids_.begin(), column_ids_.end());
  hash = common::HashUtil::CombineHashes(hash, index_scan_desc_.Hash());

  return hash;
}

bool IndexScanPlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (!AbstractScanPlanNode::operator==(rhs)) return false;

  auto &other = static_cast<const IndexScanPlanNode &>(rhs);

  if (column_ids_ != other.column_ids_) return false;
  if (index_scan_desc_ != other.index_scan_desc_) return false;

  // Index Oid
  return (index_oid_ == other.index_oid_);
}

nlohmann::json IndexScanPlanNode::ToJson() const {
  nlohmann::json j = AbstractScanPlanNode::ToJson();
  j["index_oid"] = index_oid_;
  j["column_ids"] = column_ids_;
  j["index_scan_desc"] = index_scan_desc_;
  return j;
}

void IndexScanPlanNode::FromJson(const nlohmann::json &j) {
  AbstractScanPlanNode::FromJson(j);
  index_oid_ = j.at("index_oid").get<catalog::index_oid_t>();
  column_ids_ = j.at("column_ids").get<std::vector<catalog::col_oid_t>>();
  index_scan_desc_ = j.at("index_scan_desc").get<IndexScanDesc>();
}

}  // namespace terrier::planner
