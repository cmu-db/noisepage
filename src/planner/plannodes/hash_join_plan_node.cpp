#include "planner/plannodes/hash_join_plan_node.h"
#include <memory>
#include <vector>

namespace terrier::planner {

common::hash_t HashJoinPlanNode::Hash() const {
  common::hash_t hash = AbstractJoinPlanNode::Hash();

  // Hash Join Type
  auto logical_join_type = GetLogicalJoinType();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&logical_join_type));

  // Hash Predicate
  hash = common::HashUtil::CombineHashes(hash, GetJoinPredicate()->Hash());

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

  return common::HashUtil::CombineHashes(hash, AbstractPlanNode::Hash());
}

bool HashJoinPlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (!AbstractJoinPlanNode::operator==(rhs)) {
    return false;
  }

  const auto &other = static_cast<const HashJoinPlanNode &>(rhs);

  if (GetLogicalJoinType() != other.GetLogicalJoinType()) return false;

  if (IsBloomFilterEnabled() != other.IsBloomFilterEnabled()) return false;

  if (*GetJoinPredicate() != *other.GetJoinPredicate()) return false;

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

  return GetLeftHashKeys() == other.GetLeftHashKeys() && GetRightHashKeys() == other.GetRightHashKeys() &&
         AbstractPlanNode::operator==(rhs);
}

}  // namespace terrier::planner
