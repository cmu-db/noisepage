#include "plan_node/create_view_plan_node.h"
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace terrier::plan_node {
common::hash_t CreateViewPlanNode::Hash() const {
  auto type = GetPlanNodeType();
  common::hash_t hash = common::HashUtil::Hash(&type);

  // Hash view_name
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(view_name_));

  // TODO(Gus,Wen) missing Hash for select statement

  return common::HashUtil::CombineHashes(hash, AbstractPlanNode::Hash());
}

bool CreateViewPlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (GetPlanNodeType() != rhs.GetPlanNodeType()) return false;

  auto &other = dynamic_cast<const CreateViewPlanNode &>(rhs);

  // Hash view_name
  if (GetViewName() != other.GetViewName()) return false;

  // TODO(Gus,Wen) missing == operator for select statement

  return AbstractPlanNode::operator==(rhs);
}
}  // namespace terrier::plan_node
