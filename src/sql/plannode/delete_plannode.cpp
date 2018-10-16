//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// delete_plan.cpp
//
// Identification: src/planner/delete_plan.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "planner/delete_plannode.h"

namespace terrier::sql::plannode {

void DeletePlanNode::SetParameterValues(std::vector<type::Value> *values) {
  LOG_TRACE("Setting parameter values in Delete");
  auto &children = GetChildren();
  // One sequential scan
  children[0]->SetParameterValues(values);
}

hash_t DeletePlanNode::Hash() const {
  auto type = GetPlanNodeType();
  hash_t hash = HashUtil::Hash(&type);

  hash = HashUtil::CombineHashes(hash, GetTable()->Hash());

  return HashUtil::CombineHashes(hash, AbstractPlan::Hash());
}

bool DeletePlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (GetPlanNodeType() != rhs.GetPlanNodeType())
    return false;

  auto &other = static_cast<const planner::DeletePlanNode &>(rhs);

  auto *table = GetTable();
  auto *other_table = other.GetTable();
  PELOTON_ASSERT(table && other_table);
  if (*table != *other_table)
    return false;

  return AbstractPlanNode::operator==(rhs);
}

}  // namespace planner
