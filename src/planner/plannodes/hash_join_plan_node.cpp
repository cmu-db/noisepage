#include "planner/plannodes/hash_join_plan_node.h"
#include <memory>
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
  if (!AbstractPlanNode::operator==(rhs)) return false;

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
  j["left_hash_keys"] = left_hash_keys_;
  j["right_hash_keys"] = right_hash_keys_;
  j["build_bloom_filter"] = build_bloomfilter_;
  return j;
}

void HashJoinPlanNode::FromJson(const nlohmann::json &j) {
  AbstractJoinPlanNode::FromJson(j);

  // Deserialize left keys
  auto left_keys = j.at("left_hash_keys").get<std::vector<nlohmann::json>>();
  for (const auto &key_json : left_keys) {
    if (!key_json.is_null()) {
      left_hash_keys_.push_back(parser::DeserializeExpression(key_json));
    }
  }

  // Deserialize right keys
  auto right_keys = j.at("right_hash_keys").get<std::vector<nlohmann::json>>();
  for (const auto &key_json : right_keys) {
    if (!key_json.is_null()) {
      right_hash_keys_.push_back(parser::DeserializeExpression(key_json));
    }
  }

  build_bloomfilter_ = j.at("build_bloom_filter").get<bool>();
}

}  // namespace terrier::planner
