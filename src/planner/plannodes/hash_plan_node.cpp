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

  return GetHashKeys() == other.GetHashKeys() && AbstractPlanNode::operator==(rhs);
}

}  // namespace terrier::planner
