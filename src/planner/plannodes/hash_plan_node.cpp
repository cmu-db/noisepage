#include "planner/plannodes/hash_plan_node.h"
#include <memory>
#include <vector>

namespace terrier::planner {

common::hash_t HashPlanNode::Hash() const {
  auto type = GetPlanNodeType();
  common::hash_t hash = common::HashUtil::Hash(&type);

  // Hash keys
  for (const auto &key : hash_keys_) {
    hash = common::HashUtil::CombineHashes(hash, key->Hash());
  }

  return common::HashUtil::CombineHashes(hash, AbstractPlanNode::Hash());
}

bool HashPlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (GetPlanNodeType() != rhs.GetPlanNodeType()) return false;

  const auto &other = static_cast<const HashPlanNode &>(rhs);

  // Check keys
  auto left_keys = GetHashKeys();
  auto right_keys = other.GetHashKeys();
  if (left_keys.size() != right_keys.size()) return false;
  for (size_t i = 0; i < left_keys.size(); i++) {
    if ((left_keys[i] == nullptr && right_keys[i] != nullptr) || (left_keys[i] != nullptr && right_keys[i] == nullptr))
      return false;
    if (left_keys[i] != nullptr && *left_keys[i] != *right_keys[i]) return false;
  }

  return AbstractPlanNode::operator==(rhs);
}

nlohmann::json HashPlanNode::ToJson() const {
  nlohmann::json j = AbstractPlanNode::ToJson();
  j["hash_keys"] = hash_keys_;
  return j;
}

void HashPlanNode::FromJson(const nlohmann::json &j) {
  AbstractPlanNode::FromJson(j);
  auto keys = j.at("hash_keys").get<std::vector<nlohmann::json>>();
  for (const auto &key_json : keys) {
    if (!key_json.is_null()) {
      hash_keys_.push_back(parser::DeserializeExpression(key_json));
    }
  }
}

}  // namespace terrier::planner
