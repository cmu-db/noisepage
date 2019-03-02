#include "plan_node/projection_plan_node.h"

namespace terrier::plan_node {

std::unique_ptr<AbstractPlanNode> ProjectionPlanNode::Copy() const {
  //TODO(Gus,Wen): copy schema
  ProjectionPlanNode *new_plan =
      new ProjectionPlanNode(GetOutputSchema());
  return std::unique_ptr<AbstractPlanNode>(new_plan);
}

common::hash_t ProjectionPlanNode::Hash() const {
  auto type = GetPlanNodeType();
  common::hash_t hash = common::HashUtil::Hash(&type);

  //TODO(Gus,Wen): Hash output schema

  return common::HashUtil::CombineHashes(hash, AbstractPlanNode::Hash());
}

bool ProjectionPlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (GetPlanNodeType() != rhs.GetPlanNodeType())
    return false;

  // auto &other = static_cast<const ProjectionPlanNode &>(rhs);

  //TODO(Gus,Wen): Compare output schema


  return AbstractPlanNode::operator==(rhs);
}

}  // namespace terrier::plan_node
