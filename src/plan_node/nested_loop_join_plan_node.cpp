#include "plan_node/nested_loop_join_plan_node.h"

namespace terrier::plan_node {

std::unique_ptr<AbstractPlanNode> NestedLoopJoinPlanNode::Copy() const {
  parser::AbstractExpression *predicate_copy(GetPredicate() != nullptr ? GetPredicate()->Copy().get() : nullptr);

  NestedLoopJoinPlanNode *new_plan =
      new NestedLoopJoinPlanNode(GetOutputSchema(), GetLogicalJoinType(), std::move(predicate_copy));

  return std::unique_ptr<AbstractPlanNode>(new_plan);
}

common::hash_t NestedLoopJoinPlanNode::Hash() const {
  common::hash_t hash = AbstractJoinPlanNode::Hash();

  // Hash Predicate
  hash = common::HashUtil::CombineHashes(hash, GetPredicate()->Hash());

  // Hash join type
  auto logical_join_type = GetLogicalJoinType();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&logical_join_type));

  return common::HashUtil::CombineHashes(hash, AbstractPlanNode::Hash());
}

bool NestedLoopJoinPlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (!AbstractJoinPlanNode::operator==(rhs)) {
    return false;
  }

  const auto &other = static_cast<const NestedLoopJoinPlanNode &>(rhs);

  if (GetLogicalJoinType() != other.GetLogicalJoinType()) return false;

  if (*GetPredicate() != *other.GetPredicate()) return false;

  return AbstractPlanNode::operator==(rhs);
}

}  // namespace terrier::plan_node
