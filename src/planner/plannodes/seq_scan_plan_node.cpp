#include "planner/plannodes/seq_scan_plan_node.h"
#include "common/hash_util.h"

namespace terrier::planner {

common::hash_t SeqScanPlanNode::Hash() const { return AbstractScanPlanNode::Hash(); }

bool SeqScanPlanNode::operator==(const AbstractPlanNode &rhs) const { return AbstractScanPlanNode::operator==(rhs); }

nlohmann::json SeqScanPlanNode::ToJson() const {
  nlohmann::json j = AbstractScanPlanNode::ToJson();
  j["table_oid"] = table_oid_;
  return j;
}

void SeqScanPlanNode::FromJson(const nlohmann::json &j) {
  AbstractScanPlanNode::FromJson(j);
  table_oid_ = j.at("table_oid").get<catalog::table_oid_t>();
}

}  // namespace terrier::planner
