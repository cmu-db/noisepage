#include "planner/plannodes/seq_scan_plan_node.h"
#include "common/hash_util.h"

namespace terrier::planner {

common::hash_t SeqScanPlanNode::Hash() const {
  common::hash_t hash = AbstractScanPlanNode::Hash();

  // Nothing for us to do here!

  return hash;
}

bool SeqScanPlanNode::operator==(const AbstractPlanNode &rhs) const {
  // Since this node type does not have any internal members of its own,
  // there is nothing for us to do here!
  // auto &other = static_cast<const SeqScanPlanNode &>(rhs);

  return AbstractScanPlanNode::operator==(rhs);
}

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
