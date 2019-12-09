#include <memory>
#include <utility>
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
  return left_keys_ == other.left_keys_ && right_keys_ == other.right_keys_;
}

nlohmann::json NestedLoopJoinPlanNode::ToJson() const {
  auto j = AbstractJoinPlanNode::ToJson();
  j["left_keys"] = left_keys_;
  j["right_keys"] = right_keys_;
  return j;
}

std::vector<std::unique_ptr<parser::AbstractExpression>> NestedLoopJoinPlanNode::FromJson(const nlohmann::json &j) {
  std::vector<std::unique_ptr<parser::AbstractExpression>> exprs;
  auto e1 = AbstractJoinPlanNode::FromJson(j);
  exprs.insert(exprs.end(), std::make_move_iterator(e1.begin()), std::make_move_iterator(e1.end()));

  // Deserialize left keys
  auto left_keys = j.at("left_keys").get<std::vector<nlohmann::json>>();
  for (const auto &key_json : left_keys) {
    if (!key_json.is_null()) {
      auto deserialized = parser::DeserializeExpression(key_json);
      left_keys_.emplace_back(common::ManagedPointer(deserialized.result_));
      exprs.emplace_back(std::move(deserialized.result_));
      exprs.insert(exprs.end(), std::make_move_iterator(deserialized.non_owned_exprs_.begin()),
                   std::make_move_iterator(deserialized.non_owned_exprs_.end()));
    }
  }

  // Deserialize right keys
  auto right_keys = j.at("right_keys").get<std::vector<nlohmann::json>>();
  for (const auto &key_json : right_keys) {
    if (!key_json.is_null()) {
      auto deserialized = parser::DeserializeExpression(key_json);
      right_keys_.emplace_back(common::ManagedPointer(deserialized.result_));
      exprs.emplace_back(std::move(deserialized.result_));
      exprs.insert(exprs.end(), std::make_move_iterator(deserialized.non_owned_exprs_.begin()),
                   std::make_move_iterator(deserialized.non_owned_exprs_.end()));
    }
  }
  return exprs;
}

}  // namespace terrier::planner
