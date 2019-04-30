#include "planner/plannodes/projection_plan_node.h"
#include <memory>

namespace terrier::planner {

common::hash_t ProjectionPlanNode::Hash() const {
  auto type = GetPlanNodeType();
  common::hash_t hash = common::HashUtil::Hash(&type);

  // TODO(Gus,Wen): Hash output schema

  return common::HashUtil::CombineHashes(hash, AbstractPlanNode::Hash());
}

bool ProjectionPlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (GetPlanNodeType() != rhs.GetPlanNodeType()) return false;

  // auto &other = static_cast<const ProjectionPlanNode &>(rhs);

  // TODO(Gus,Wen): Compare output schema

  return AbstractPlanNode::operator==(rhs);
}

}  // namespace terrier::planner
