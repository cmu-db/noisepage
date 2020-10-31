#include "planner/plannodes/hash_join_plan_node.h"

#include <memory>
#include <utility>
#include <vector>

#include "common/json.h"
#include "planner/plannodes/output_schema.h"

namespace noisepage::planner {

std::unique_ptr<HashJoinPlanNode> HashJoinPlanNode::Builder::Build() {
  return std::unique_ptr<HashJoinPlanNode>(new HashJoinPlanNode(std::move(children_), std::move(output_schema_),
                                                                join_type_, join_predicate_, std::move(left_hash_keys_),
                                                                std::move(right_hash_keys_)));
}

HashJoinPlanNode::HashJoinPlanNode(std::vector<std::unique_ptr<AbstractPlanNode>> &&children,
                                   std::unique_ptr<OutputSchema> output_schema, LogicalJoinType join_type,
                                   common::ManagedPointer<parser::AbstractExpression> predicate,
                                   std::vector<common::ManagedPointer<parser::AbstractExpression>> &&left_hash_keys,
                                   std::vector<common::ManagedPointer<parser::AbstractExpression>> &&right_hash_keys)
    : AbstractJoinPlanNode(std::move(children), std::move(output_schema), join_type, predicate),
      left_hash_keys_(std::move(left_hash_keys)),
      right_hash_keys_(std::move(right_hash_keys)) {}

common::hash_t HashJoinPlanNode::Hash() const {
  common::hash_t hash = AbstractJoinPlanNode::Hash();

  // Hash left keys
  for (const auto &left_hash_key : left_hash_keys_) {
    hash = common::HashUtil::CombineHashes(hash, left_hash_key->Hash());
  }

  // Hash right keys
  for (const auto &right_hash_key : right_hash_keys_) {
    hash = common::HashUtil::CombineHashes(hash, right_hash_key->Hash());
  }

  return hash;
}

bool HashJoinPlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (!AbstractJoinPlanNode::operator==(rhs)) return false;

  const auto &other = static_cast<const HashJoinPlanNode &>(rhs);

  // Left hash keys
  if (left_hash_keys_.size() != other.left_hash_keys_.size()) return false;
  for (size_t i = 0; i < left_hash_keys_.size(); i++) {
    if (*left_hash_keys_[i] != *other.left_hash_keys_[i]) return false;
  }

  // Right hash keys
  if (right_hash_keys_.size() != other.right_hash_keys_.size()) return false;
  for (size_t i = 0; i < right_hash_keys_.size(); i++) {
    if (*right_hash_keys_[i] != *other.right_hash_keys_[i]) return false;
  }

  return true;
}

nlohmann::json HashJoinPlanNode::ToJson() const {
  nlohmann::json j = AbstractJoinPlanNode::ToJson();
  j["left_hash_keys"] = left_hash_keys_;
  j["right_hash_keys"] = right_hash_keys_;
  return j;
}

std::vector<std::unique_ptr<parser::AbstractExpression>> HashJoinPlanNode::FromJson(const nlohmann::json &j) {
  std::vector<std::unique_ptr<parser::AbstractExpression>> exprs;
  auto e1 = AbstractJoinPlanNode::FromJson(j);
  exprs.insert(exprs.end(), std::make_move_iterator(e1.begin()), std::make_move_iterator(e1.end()));

  // Deserialize left keys
  auto left_keys = j.at("left_hash_keys").get<std::vector<nlohmann::json>>();
  for (const auto &key_json : left_keys) {
    if (!key_json.is_null()) {
      auto deserialized = parser::DeserializeExpression(key_json);
      left_hash_keys_.emplace_back(common::ManagedPointer(deserialized.result_));
      exprs.emplace_back(std::move(deserialized.result_));
      exprs.insert(exprs.end(), std::make_move_iterator(deserialized.non_owned_exprs_.begin()),
                   std::make_move_iterator(deserialized.non_owned_exprs_.end()));
    }
  }

  // Deserialize right keys
  auto right_keys = j.at("right_hash_keys").get<std::vector<nlohmann::json>>();
  for (const auto &key_json : right_keys) {
    if (!key_json.is_null()) {
      auto deserialized = parser::DeserializeExpression(key_json);
      right_hash_keys_.emplace_back(common::ManagedPointer(deserialized.result_));
      exprs.emplace_back(std::move(deserialized.result_));
      exprs.insert(exprs.end(), std::make_move_iterator(deserialized.non_owned_exprs_.begin()),
                   std::make_move_iterator(deserialized.non_owned_exprs_.end()));
    }
  }

  return exprs;
}

DEFINE_JSON_BODY_DECLARATIONS(HashJoinPlanNode);

}  // namespace noisepage::planner
