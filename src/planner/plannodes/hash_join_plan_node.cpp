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
  auto build_bloomfilter = IsBloomFilterEnabled();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&build_bloomfilter));

  return hash;
}

bool HashJoinPlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (!AbstractJoinPlanNode::operator==(rhs)) return false;

  const auto &other = static_cast<const HashJoinPlanNode &>(rhs);

  if (GetLogicalJoinType() != other.GetLogicalJoinType()) return false;

  if (IsBloomFilterEnabled() != other.IsBloomFilterEnabled()) return false;

  // Left hash keys
  const auto &left_keys = GetLeftHashKeys();
  const auto &left_other_keys = other.GetLeftHashKeys();
  if (left_keys.size() != left_other_keys.size()) {
    return false;
  }

  for (size_t i = 0; i < left_keys.size(); i++) {
    if (*left_keys[i] != *left_other_keys[i]) {
      return false;
    }
  }

  // Right hash keys
  const auto &right_keys = GetRightHashKeys();
  const auto &right_other_keys = other.GetRightHashKeys();
  if (right_keys.size() != right_other_keys.size()) {
    return false;
  }

  for (size_t i = 0; i < right_keys.size(); i++) {
    if (*right_keys[i] != *right_other_keys[i]) {
      return false;
    }
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
