#include "planner/plannodes/hash_plan_node.h"
#include <memory>
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
  if (GetPlanNodeType() != rhs.GetPlanNodeType()) return false;

  const auto &other = static_cast<const HashPlanNode &>(rhs);

  // Check keys
  if (hash_keys_ != other.hash_keys_) return false;

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
