#include "planner/plannodes/set_op_plan_node.h"
#include <memory>
#include <utility>

namespace terrier::planner {

common::hash_t SetOpPlanNode::Hash() const {
  auto type = GetPlanNodeType();
  common::hash_t hash = common::HashUtil::Hash(&type);

  // Hash set_op
  auto set_op = GetSetOp();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&set_op));

  return common::HashUtil::CombineHashes(hash, AbstractPlanNode::Hash());
}

bool SetOpPlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (GetPlanNodeType() != rhs.GetPlanNodeType()) return false;

  auto &other = dynamic_cast<const SetOpPlanNode &>(rhs);

  // Set op
  if (GetSetOp() != other.GetSetOp()) return false;

  return AbstractPlanNode::operator==(rhs);
}
}  // namespace terrier::planner
