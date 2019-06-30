#include "planner/plannodes/index_join_plan_node.h"
#include "common/hash_util.h"

namespace terrier::planner {

common::hash_t IndexJoinPlanNode::Hash() const { return AbstractJoinPlanNode::Hash(); }

bool IndexJoinPlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (!AbstractJoinPlanNode::operator==(rhs)) return false;

  const auto &other = static_cast<const IndexJoinPlanNode &>(rhs);
  return other.table_oid_ == table_oid_ && other.index_oid_ == index_oid_;
}

nlohmann::json IndexJoinPlanNode::ToJson() const {
  nlohmann::json j = AbstractJoinPlanNode::ToJson();
  j["index_oid"] = index_oid_;
  j["table_oid"] = table_oid_;
  return j;
}

void IndexJoinPlanNode::FromJson(const nlohmann::json &j) {
  AbstractJoinPlanNode::FromJson(j);
  index_oid_ = j.at("index_oid").get<catalog::index_oid_t>();
  table_oid_ = j.at("table_oid").get<catalog::table_oid_t>();
}

}  // namespace terrier::planner
