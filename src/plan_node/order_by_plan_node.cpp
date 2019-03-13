#include "plan_node/order_by_plan_node.h"

namespace terrier::plan_node {

std::unique_ptr<AbstractPlanNode> OrderByPlanNode::Copy() const {
  if (HasLimit()) {
    return std::unique_ptr<AbstractPlanNode>(
        new OrderByPlanNode(GetOutputSchema(), sort_keys_, sort_key_orderings_, limit_, offset_));
  }
  return std::unique_ptr<AbstractPlanNode>(new OrderByPlanNode(GetOutputSchema(), sort_keys_, sort_key_orderings_));
}

common::hash_t OrderByPlanNode::Hash() const {
  auto type = GetPlanNodeType();
  common::hash_t hash = common::HashUtil::Hash(&type);

  for (const catalog::col_oid_t sort_key : GetSortKeys()) {
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&sort_key));
  }

  for (const OrderByOrdering flag : GetSortKeyOrderings()) {
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&flag));
  }

  hash = common::HashUtil::CombineHashes(hash, GetOutputSchema()->Hash());

  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&has_limit_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&limit_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&offset_));

  return common::HashUtil::CombineHashes(hash, AbstractPlanNode::Hash());
}

bool OrderByPlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (GetPlanNodeType() != rhs.GetPlanNodeType()) {
    return false;
  }

  auto &other = static_cast<const OrderByPlanNode &>(rhs);

  // Sort Keys
  if (GetSortKeys() != other.GetSortKeys()) {
    return false;
  }

  // Descend Flags
  if (GetSortKeyOrderings() != other.GetSortKeyOrderings()) {
    return false;
  }

  // TODO(Gus,Wen): Check equaility of output schema

  // Limit/Offset
  if (HasLimit() != other.HasLimit() || GetOffset() != other.GetOffset() || GetLimit() != other.GetLimit()) {
    return false;
  }

  return AbstractPlanNode::operator==(rhs);
}

}  // namespace terrier::plan_node
