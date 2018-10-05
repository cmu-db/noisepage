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

AbstractPlanNode::AbstractPlanNode() {}

AbstractPlanNode::~AbstractPlanNode() {}

void AbstractPlanNode::AddChild(std::unique_ptr<AbstractPlanNode> &&child) { children_.emplace_back(std::move(child)); }

const std::vector<std::unique_ptr<AbstractPlanNode>> &AbstractPlanNode::GetChildren() const { return children_; }

const AbstractPlanNode *AbstractPlanNode::GetChild(uint32_t child_index) const {
  PELOTON_ASSERT(child_index < children_.size());
  return children_[child_index].get();
}

const AbstractPlanNode *AbstractPlanNode::GetParent() const { return parent_; }

hash_t AbstractPlanNode::Hash() const {
  hash_t hash = 0;
  for (auto &child : GetChildren()) {
    hash = HashUtil::CombineHashes(hash, child->Hash());
  }
  return hash;
}

bool AbstractPlanNode::operator==(const AbstractPlanNode &rhs) const {
  auto num = GetChildren().size();
  if (num != rhs.GetChildren().size()) return false;
  for (unsigned int i = 0; i < num; i++) {
    if (*GetChild(i) != *(AbstractPlanNode *)rhs.GetChild(i)) return false;
  }
  return true;
}

}  // namespace terrier::sql::plannode
