#include "planner/plannodes/abstract_join_plan_node.h"

namespace terrier::planner {

bool AbstractJoinPlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (!AbstractPlanNode::operator==(rhs)) return false;

  // Check join type
  auto &other = dynamic_cast<const AbstractJoinPlanNode &>(rhs);
  if (GetLogicalJoinType() != other.GetLogicalJoinType()) {
    return false;
  }

  // Check predicate
  auto &pred = GetJoinPredicate();
  auto &other_pred = other.GetJoinPredicate();
  if ((pred == nullptr && other_pred != nullptr) || (pred != nullptr && other_pred == nullptr)) {
    return false;
  }
  if (pred != nullptr && *pred != *other_pred) {
    return false;
  }

  return true;
}

common::hash_t AbstractJoinPlanNode::Hash() const {
  common::hash_t hash = AbstractPlanNode::Hash();

  auto join_type = GetLogicalJoinType();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&join_type));

  if (GetJoinPredicate() != nullptr) {
    hash = common::HashUtil::CombineHashes(hash, GetJoinPredicate()->Hash());
  }

  return hash;
}

nlohmann::json AbstractJoinPlanNode::ToJson() const {
  nlohmann::json j = AbstractPlanNode::ToJson();
  j["join_type"] = join_type_;
  j["join_predicate"] = join_predicate_;
  return j;
}

void AbstractJoinPlanNode::FromJson(const nlohmann::json &j) {
  AbstractPlanNode::FromJson(j);
  join_type_ = j.at("join_type").get<LogicalJoinType>();
  if (!j.at("join_predicate").is_null()) {
    join_predicate_ = parser::DeserializeExpression(j.at("join_predicate"));
  }
}

}  // namespace terrier::planner
