#include "planner/plannodes/abstract_join_plan_node.h"

namespace terrier::planner {

bool AbstractJoinPlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (!AbstractPlanNode::operator==(rhs)) return false;

  auto &other = dynamic_cast<const AbstractJoinPlanNode &>(rhs);

  // Check join type
  if (join_type_ != other.join_type_) return false;

  // Check predicate
  if ((join_predicate_ == nullptr && other.join_predicate_ != nullptr) || (join_predicate_ != nullptr && other.join_predicate_ == nullptr)) {
    return false;
  }
  if (join_predicate_ != nullptr && *join_predicate_ != *other.join_predicate_) {
    return false;
  }

  return true;
}

common::hash_t AbstractJoinPlanNode::Hash() const {
  common::hash_t hash = AbstractPlanNode::Hash();

  // Join Type
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(join_type_));

  // Predicate
  if (join_predicate_ != nullptr) {
    hash = common::HashUtil::CombineHashes(hash, join_predicate_->Hash());
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
