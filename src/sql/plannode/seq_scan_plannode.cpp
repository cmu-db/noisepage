//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// seq_scan_plan.cpp
//
// Identification: src/planner/seq_scan_plan.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "sql/plannode/seq_scan_plannode.h"

#include "common/macros.h"

namespace terrier::sql::plannode {

hash_t SeqScanPlanNode::Hash() const {
  auto type = GetPlanNodeType();
  hash_t hash = HashUtil::Hash(&type);

  hash = HashUtil::CombineHashes(hash, GetTable()->Hash());
  if (GetPredicate() != nullptr) {
    hash = HashUtil::CombineHashes(hash, GetPredicate()->Hash());
  }

  for (auto &column_id : GetColumnIds()) {
    hash = HashUtil::CombineHashes(hash, HashUtil::Hash(&column_id));
  }

  auto is_update = IsForUpdate();
  hash = HashUtil::CombineHashes(hash, HashUtil::Hash(&is_update));

  return HashUtil::CombineHashes(hash, AbstractPlan::Hash());
}

bool SeqScanPlanNode::operator==(const AbstractPlan &rhs) const {
  if (GetPlanNodeType() != rhs.GetPlanNodeType()) return false;

  auto &other = static_cast<const planner::SeqScanPlanNode &>(rhs);
  auto *table = GetTable();
  auto *other_table = other.GetTable();
  TERRIER_ASSERT(table, "Unexpected null table");
  TERRIER_ASSERT(other_table, "Unexpected null table");
  if (*table != *other_table) return false;

  // Predicate
  auto *pred = GetPredicate();
  auto *other_pred = other.GetPredicate();
  if ((pred == nullptr && other_pred != nullptr) || (pred != nullptr && other_pred == nullptr)) return false;
  if (pred && *pred != *other_pred) return false;

  // Column Ids
  size_t column_id_count = GetColumnIds().size();
  if (column_id_count != other.GetColumnIds().size()) return false;
  for (size_t i = 0; i < column_id_count; i++) {
    if (GetColumnIds()[i] != other.GetColumnIds()[i]) {
      return false;
    }
  }

  if (IsForUpdate() != other.IsForUpdate()) return false;

  return AbstractPlan::operator==(rhs);
}

}  // namespace terrier::sql::plannode
