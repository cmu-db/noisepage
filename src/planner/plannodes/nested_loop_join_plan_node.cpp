#include <memory>
#include <vector>

#include "planner/plannodes/nested_loop_join_plan_node.h"

namespace terrier::planner {

common::hash_t NestedLoopJoinPlanNode::Hash() const {
  common::hash_t hash = AbstractJoinPlanNode::Hash();
  hash = common::HashUtil::CombineHashInRange(hash, left_keys_.begin(), left_keys_.end());
  hash = common::HashUtil::CombineHashInRange(hash, right_keys_.begin(), right_keys_.end());
  return hash;
}

bool NestedLoopJoinPlanNode::operator==(const AbstractPlanNode &rhs) const {
  // There is nothing else for us to do here! Go home! You're drunk!
  // Unfortunately, now there is something to do...
  if (!AbstractJoinPlanNode::operator==(rhs)) return false;

  auto &other = reinterpret_cast<const NestedLoopJoinPlanNode &>(rhs);
  if (left_keys_ != other.left_keys_) return false;
  return right_keys_ == other.right_keys_;
}

nlohmann::json NestedLoopJoinPlanNode::ToJson() const {
  auto j = AbstractJoinPlanNode::ToJson();
  j["left_keys"] = left_keys_;
  j["right_keys"] = right_keys_;
  return j;
}

void NestedLoopJoinPlanNode::FromJson(const nlohmann::json &j) {
  AbstractJoinPlanNode::FromJson(j);
  auto left_keys = j.at("left_keys").get<std::vector<nlohmann::json>>();
  for (const auto &json : left_keys) {
    left_keys_.push_back(parser::DeserializeExpression(json));
  }

  auto right_keys = j.at("right_keys").get<std::vector<nlohmann::json>>();
  for (const auto &json : right_keys) {
    right_keys_.push_back(parser::DeserializeExpression(json));
  }
}

}  // namespace terrier::planner
