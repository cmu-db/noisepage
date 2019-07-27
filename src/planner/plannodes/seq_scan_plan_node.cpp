#include <vector>

#include "common/hash_util.h"
#include "planner/plannodes/seq_scan_plan_node.h"

namespace terrier::planner {

common::hash_t SeqScanPlanNode::Hash() const {
  common::hash_t hash = AbstractScanPlanNode::Hash();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(table_oid_));
  hash = common::HashUtil::CombineHashInRange(hash, column_ids_.begin(), column_ids_.end());
  return hash;
}

bool SeqScanPlanNode::operator==(const AbstractPlanNode &rhs) const {
  auto &other = static_cast<const SeqScanPlanNode &>(rhs);
  if (!AbstractScanPlanNode::operator==(rhs)) return false;
  if (table_oid_ != other.table_oid_) return false;
  return column_ids_ == other.column_ids_;
}

nlohmann::json SeqScanPlanNode::ToJson() const {
  nlohmann::json j = AbstractScanPlanNode::ToJson();
  j["column_ids"] = column_ids_;
  j["table_oid"] = table_oid_;
  return j;
}

void SeqScanPlanNode::FromJson(const nlohmann::json &j) {
  AbstractScanPlanNode::FromJson(j);
  column_ids_ = j.at("column_ids").get<std::vector<catalog::col_oid_t>>();
  table_oid_ = j.at("table_oid").get<catalog::table_oid_t>();
}

}  // namespace terrier::planner
