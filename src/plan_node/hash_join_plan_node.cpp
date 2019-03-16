#include <memory>
#include <vector>
#include "plan_node/hash_join_plan_node.h"

namespace terrier::plan_node {

std::unique_ptr<AbstractPlanNode> HashJoinPlanNode::Copy() const {
  // Copy predicate
  parser::AbstractExpression *predicate_copy(GetPredicate() != nullptr ? GetPredicate()->Copy().get() : nullptr);

  // TODO(Gus,Wen): Copy output schema

  // Left and right hash keys
  std::vector<parser::AbstractExpression *> left_hash_keys_copy, right_hash_keys_copy;
  for (const auto &left_hash_key : left_hash_keys_) {
    left_hash_keys_copy.emplace_back(left_hash_key->Copy().get());
  }
  for (const auto &right_hash_key : right_hash_keys_) {
    right_hash_keys_copy.emplace_back(right_hash_key->Copy().get());
  }

  // Create plan copy
  auto *new_plan = new HashJoinPlanNode(GetOutputSchema(), GetLogicalJoinType(), predicate_copy, left_hash_keys_copy,
                                        right_hash_keys_copy, IsBloomFilterEnabled());
  return std::unique_ptr<AbstractPlanNode>(new_plan);
}

common::hash_t HashJoinPlanNode::Hash() const {
  common::hash_t hash = AbstractJoinPlanNode::Hash();

  // Hash Join Type
  auto logical_join_type = GetLogicalJoinType();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&logical_join_type));

  // Hash Predicate
  hash = common::HashUtil::CombineHashes(hash, GetPredicate()->Hash());

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

  // TODO(Gus,Wen): hash output schema

  return common::HashUtil::CombineHashes(hash, AbstractPlanNode::Hash());
}

bool HashJoinPlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (!AbstractJoinPlanNode::operator==(rhs)) {
    return false;
  }

  const auto &other = static_cast<const HashJoinPlanNode &>(rhs);

  if (GetLogicalJoinType() != other.GetLogicalJoinType()) return false;

  if (IsBloomFilterEnabled() != other.IsBloomFilterEnabled()) return false;

  if (*GetPredicate() != *other.GetPredicate()) return false;

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

  // TODO(Gus,Wen): Compare equality of schema

  return GetLeftHashKeys() == other.GetLeftHashKeys() && GetRightHashKeys() == other.GetRightHashKeys() &&
         AbstractPlanNode::operator==(rhs);
}

}  // namespace terrier::plan_node
