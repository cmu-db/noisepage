#include "planner/plannodes/hash_join_plan_node.h"

#include <memory>
#include <utility>
#include <vector>

namespace terrier::planner {

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

  // Hash bloom filter enabled
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(build_bloomfilter_));

  return hash;
}

bool HashJoinPlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (!AbstractJoinPlanNode::operator==(rhs)) return false;

  const auto &other = static_cast<const HashJoinPlanNode &>(rhs);

  // Bloom Filter Flag
  if (build_bloomfilter_ != other.build_bloomfilter_) return false;

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
  std::vector<nlohmann::json> left_hash_keys;
  left_hash_keys.reserve(left_hash_keys_.size());
  for (const auto &key : left_hash_keys_) {
    left_hash_keys.emplace_back(key->ToJson());
  }
  j["left_hash_keys"] = left_hash_keys;
  std::vector<nlohmann::json> right_hash_keys;
  right_hash_keys.reserve(right_hash_keys_.size());
  for (const auto &key : right_hash_keys_) {
    right_hash_keys.emplace_back(key->ToJson());
  }
  j["right_hash_keys"] = right_hash_keys;
  j["build_bloom_filter"] = build_bloomfilter_;
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

  build_bloomfilter_ = j.at("build_bloom_filter").get<bool>();
  return exprs;
}

}  // namespace terrier::planner
