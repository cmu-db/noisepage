#include "plan_node/materialization_plan_node.h"
#include <memory>
#include <utility>

namespace terrier::plan_node {

common::hash_t MaterializationPlanNode::Hash() const {
  auto type = GetPlanNodeType();
  common::hash_t hash = common::HashUtil::Hash(&type);

  // Hash materialize_flag
  auto materialize_flag = GetMaterializeFlag();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&materialize_flag));

  return common::HashUtil::CombineHashes(hash, AbstractPlanNode::Hash());
}

bool MaterializationPlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (GetPlanNodeType() != rhs.GetPlanNodeType()) return false;

  auto &other = dynamic_cast<const MaterializationPlanNode &>(rhs);

  // Set op
  if (GetMaterializeFlag() != other.GetMaterializeFlag()) return false;

  return AbstractPlanNode::operator==(rhs);
}
}  // namespace terrier::plan_node
