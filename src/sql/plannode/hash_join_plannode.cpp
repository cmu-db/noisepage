//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// hash_join_plan.cpp
//
// Identification: src/planner/hash_join_plan.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <string>
#include <vector>
#include <sql/plannode/hash_join_plannode.h>
#include "planner/hash_join_plannode.h"

namespace terrier::sql::plannode {

void HashJoinPlanNode::GetLeftHashKeys(
    std::vector<const expression::AbstractExpression *> &keys) const {
  for (const auto &left_key : left_hash_keys_) {
    keys.push_back(left_key.get());
  }
}

void HashJoinPlan::GetRightHashKeys(
    std::vector<const expression::AbstractExpression *> &keys) const {
  for (const auto &right_key : right_hash_keys_) {
    keys.push_back(right_key.get());
  }
}
//
//void HashJoinPlan::HandleSubplanBinding(bool is_left,
//                                        const BindingContext &input) {
//  auto &keys = is_left ? left_hash_keys_ : right_hash_keys_;
//  for (auto &key : keys) {
//    auto *key_exp = const_cast<expression::AbstractExpression *>(key.get());
//    key_exp->PerformBinding({&input});
//  }
//}

std::unique_ptr<AbstractPlanNode> HashJoinPlanNode::Copy() const {
  // Predicate
  ExpressionPtr predicate_copy(GetPredicate() ? GetPredicate()->Copy()
                                              : nullptr);

  // Schema
  std::shared_ptr<const catalog::Schema> schema_copy(
      catalog::Schema::CopySchema(GetSchema()));

  // Left and right hash keys
  std::vector<ExpressionPtr> left_hash_keys_copy, right_hash_keys_copy;
  for (const auto &left_hash_key : left_hash_keys_) {
    left_hash_keys_copy.emplace_back(left_hash_key->Copy());
  }
  for (const auto &right_hash_key : right_hash_keys_) {
    right_hash_keys_copy.emplace_back(right_hash_key->Copy());
  }

  // Create plan copy
  auto *new_plan =
      new HashJoinPlanNode(GetJoinType(), std::move(predicate_copy),
                       GetProjInfo()->Copy(), schema_copy, left_hash_keys_copy,
                       right_hash_keys_copy, build_bloomfilter_);
  return std::unique_ptr<AbstractPlanNode>(new_plan);
}

common::hash_t HashJoinPlanNode::Hash() const {
  common::hash_t hash = AbstractJoinPlanNode::Hash();

  std::vector<const expression::AbstractExpression *> keys;
  GetLeftHashKeys(keys);
  for (size_t i = 0; i < keys.size(); i++) {
    hash = common::HashUtil::CombineHashes(hash, keys[i]->Hash());
  }

  keys.clear();
  GetRightHashKeys(keys);
  for (size_t i = 0; i < keys.size(); i++) {
    hash = common::HashUtil::CombineHashes(hash, keys[i]->Hash());
  }

  return common::HashUtil::CombineHashes(hash, AbstractPlanNode::Hash());
}

bool HashJoinPlan::operator==(const AbstractPlanNode &rhs) const {
  if (!AbstractJoinPlanNode::operator==(rhs)) {
    return false;
  }

  const auto &other = static_cast<const HashJoinPlanNode &>(rhs);

  std::vector<const expression::AbstractExpression *> keys, other_keys;

  // Left hash keys
  GetLeftHashKeys(keys);
  other.GetLeftHashKeys(other_keys);
  size_t keys_count = keys.size();
  if (keys_count != other_keys.size()) {
    return false;
  }

  for (size_t i = 0; i < keys_count; i++) {
    if (*keys[i] != *other_keys[i]) {
      return false;
    }
  }

  keys.clear();
  other_keys.clear();

  // Right hash keys
  GetRightHashKeys(keys);
  other.GetRightHashKeys(other_keys);
  keys_count = keys.size();
  if (keys_count != other_keys.size()) {
    return false;
  }

  for (size_t i = 0; i < keys_count; i++) {
    if (*keys[i] != *other_keys[i]) {
      return false;
    }
  }

  return AbstractPlanNode::operator==(rhs);
}
}  // namespace terrier::sql::plannode
