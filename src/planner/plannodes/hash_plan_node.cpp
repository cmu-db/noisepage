#include "planner/plannodes/hash_plan_node.h"

#include <memory>
#include <utility>
#include <vector>

namespace terrier::planner {

common::hash_t HashPlanNode::Hash() const {
  common::hash_t hash = AbstractPlanNode::Hash();

  // Hash keys
  for (const auto &key : hash_keys_) {
    hash = common::HashUtil::CombineHashes(hash, key->Hash());
  }

  return hash;
}

bool HashPlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (!AbstractPlanNode::operator==(rhs)) return false;

  const auto &other = static_cast<const HashPlanNode &>(rhs);

  // Hash keys
  if (hash_keys_.size() != other.hash_keys_.size()) return false;
  for (size_t i = 0; i < hash_keys_.size(); i++) {
    if (*hash_keys_[i] != *other.hash_keys_[i]) return false;
  }

  return true;
}

nlohmann::json HashPlanNode::ToJson() const {
  nlohmann::json j = AbstractPlanNode::ToJson();
  std::vector<nlohmann::json> hash_keys;
  for (const auto &key : hash_keys_) {
    hash_keys.emplace_back(key->ToJson());
  }
  j["hash_keys"] = hash_keys;
  return j;
}

std::vector<std::unique_ptr<parser::AbstractExpression>> HashPlanNode::FromJson(const nlohmann::json &j) {
  std::vector<std::unique_ptr<parser::AbstractExpression>> exprs;
  auto e1 = AbstractPlanNode::FromJson(j);
  exprs.insert(exprs.end(), std::make_move_iterator(e1.begin()), std::make_move_iterator(e1.end()));
  auto keys = j.at("hash_keys").get<std::vector<nlohmann::json>>();
  for (const auto &key_json : keys) {
    if (!key_json.is_null()) {
      auto deserialized = parser::DeserializeExpression(key_json);
      hash_keys_.emplace_back(common::ManagedPointer(deserialized.result_));
      exprs.emplace_back(std::move(deserialized.result_));
      exprs.insert(exprs.end(), std::make_move_iterator(deserialized.non_owned_exprs_.begin()),
                   std::make_move_iterator(deserialized.non_owned_exprs_.end()));
    }
  }
  return exprs;
}

}  // namespace terrier::planner
