#include "planner/plannodes/projection_plan_node.h"
#include <memory>

namespace terrier::planner {

common::hash_t ProjectionPlanNode::Hash() const {
  common::hash_t hash = AbstractPlanNode::Hash();

  // Nothing for us to do here!

  return hash;
}

bool ProjectionPlanNode::operator==(const AbstractPlanNode &rhs) const {
  // Since this node type does not have any internal members of its own,
  // there is nothing for us to do here!

  return (AbstractPlanNode::operator==(rhs));
}

nlohmann::json ProjectionPlanNode::ToJson() const {
  nlohmann::json j = AbstractPlanNode::ToJson();
  return j;
}

void ProjectionPlanNode::FromJson(const nlohmann::json &j) { AbstractPlanNode::FromJson(j); }

}  // namespace terrier::planner
