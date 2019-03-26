#include "plan_node/result_plan_node.h"
#include <memory>
#include <utility>

namespace terrier::plan_node {

common::hash_t ResultPlanNode::Hash() const {
  auto type = GetPlanNodeType();
  common::hash_t hash = common::HashUtil::Hash(&type);

  // TODO(Gus,Wen) hash tuple

  return common::HashUtil::CombineHashes(hash, AbstractPlanNode::Hash());
}

bool ResultPlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (GetPlanNodeType() != rhs.GetPlanNodeType()) return false;

  auto &other = dynamic_cast<const ResultPlanNode &>(rhs);

  // TODO(Gus,Wen) compare tuple

  return AbstractPlanNode::operator==(rhs);
}
}  // namespace terrier::plan_node
