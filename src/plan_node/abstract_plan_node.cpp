#include "plan_node/abstract_plan_node.h"

namespace terrier::plan_node {

AbstractPlanNode::AbstractPlanNode(catalog::Schema output_schema) : output_schema_(output_schema) {}

AbstractPlanNode::~AbstractPlanNode() {}

void AbstractPlanNode::AddChild(std::unique_ptr<AbstractPlanNode> &&child) { children_.emplace_back(std::move(child)); }

const std::vector<std::unique_ptr<AbstractPlanNode>> &AbstractPlanNode::GetChildren() const { return children_; }

const AbstractPlanNode *AbstractPlanNode::GetChild(uint32_t child_index) const {
  TERRIER_ASSERT(child_index < children_.size(),
                 "index into children of plan node should be less than number of children");
  return children_[child_index].get();
}

common::hash_t AbstractPlanNode::Hash() const {
  common::hash_t hash = 0;
  for (auto &child : GetChildren()) {
    hash = common::HashUtil::CombineHashes(hash, child->Hash());
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

}  // namespace terrier::plan_node
