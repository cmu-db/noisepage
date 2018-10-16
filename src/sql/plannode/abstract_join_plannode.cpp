//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// abstract_plan.cpp
//
// Identification: src/planner/abstract_join_plan.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "planner/abstract_join_plannode.h"
#include <numeric>
#include "common/hash_util.h"

namespace terrier::sql::plannode {

void AbstractJoinPlanNode::GetOutputColumns(std::vector<oid_t> &columns) const {
  columns.resize(GetSchema()->GetColumnCount());
  std::iota(columns.begin(), columns.end(), 0);
}

bool AbstractJoinPlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (GetPlanNodeType() != rhs.GetPlanNodeType()) {
    return false;
  }

  // Check join type
  auto &other = static_cast<const planner::AbstractJoinPlanNode &>(rhs);
  if (GetJoinType() != other.GetJoinType()) {
    return false;
  }

  // Check predicate
  auto *pred = GetPredicate();
  auto *other_pred = other.GetPredicate();
  if ((pred == nullptr && other_pred != nullptr) ||
      (pred != nullptr && other_pred == nullptr)) {
    return false;
  }
  if (pred != nullptr && *pred != *other_pred) {
    return false;
  }

  // Check projection information
  auto *proj_info = GetProjInfo();
  auto *other_proj_info = other.GetProjInfo();
  if ((proj_info == nullptr && other_proj_info != nullptr) ||
      (proj_info != nullptr && other_proj_info == nullptr)) {
    return false;
  }
  if (proj_info != nullptr && *proj_info != *other_proj_info) {
    return false;
  }

  // Looks okay, but let subclass make sure
  return true;
}

common::hash_t AbstractJoinPlanNode::Hash() const {
  auto type = GetPlanNodeType();
  common::hash_t hash = common::HashUtil::Hash(&type);

  auto join_type = GetJoinType();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&join_type));

  if (GetPredicate() != nullptr) {
    hash = common::HashUtil::CombineHashes(hash, GetPredicate()->Hash());
  }

  if (GetProjInfo() != nullptr) {
    hash = common::HashUtil::CombineHashes(hash, GetProjInfo()->Hash());
  }

  return hash;
}

}  // namespace terrier::sql::plannode
