//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// abstract_plan.cpp
//
// Identification: src/sql/plannode/abstract_plannode.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "sql/plannode/abstract_plannode.h"

#include "common/macros.h"
#include "util/hash_util.h"

namespace terrier::sql::plannode {

AbstractPlan::AbstractPlan() {}

AbstractPlan::~AbstractPlan() {}

void AbstractPlan::AddChild(std::unique_ptr<AbstractPlan> &&child) {
  children_.emplace_back(std::move(child));
}

const std::vector<std::unique_ptr<AbstractPlan>> &AbstractPlan::GetChildren()
    const {
  return children_;
}

const AbstractPlan *AbstractPlan::GetChild(uint32_t child_index) const {
  PELOTON_ASSERT(child_index < children_.size());
  return children_[child_index].get();
}

const AbstractPlan *AbstractPlan::GetParent() const { return parent_; }

hash_t AbstractPlan::Hash() const {
  hash_t hash = 0;
  for (auto &child : GetChildren()) {
    hash = HashUtil::CombineHashes(hash, child->Hash());
  }
  return hash;
}

bool AbstractPlan::operator==(const AbstractPlan &rhs) const {
  auto num = GetChildren().size();
  if (num != rhs.GetChildren().size())
    return false;
  for (unsigned int i = 0; i < num; i++) {
    if (*GetChild(i) != *(AbstractPlan *)rhs.GetChild(i))
      return false;
  }
  return true;
}

}  // namespace terrier::sql::plannode
