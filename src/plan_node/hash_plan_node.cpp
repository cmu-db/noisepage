#include "plan_node/hash_plan_node.h"

namespace terrier::plan_node {

common::hash_t HashPlanNode::Hash() const {
  auto type = GetPlanNodeType();
  common::hash_t hash = common::HashUtil::Hash(&type);

  for (auto &hash_key : hash_keys_) hash = common::HashUtil::CombineHashes(hash, hash_key->Hash());

  return common::HashUtil::CombineHashes(hash, AbstractPlanNode::Hash());
}

bool HashPlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (GetPlanNodeType() != rhs.GetPlanNodeType()) return false;

  auto &other = static_cast<const HashPlanNode &>(rhs);
  auto hash_key_size = GetHashKeys().size();
  if (hash_key_size != other.GetHashKeys().size()) return false;

  for (size_t i = 0; i < hash_key_size; i++) {
    if (*GetHashKeys()[i].get() != *other.GetHashKeys()[i].get()) return false;
  }

  return AbstractPlanNode::operator==(rhs);
}

}  // namespace terrier::plan_node
