#include "planner/plannodes/projection_plan_node.h"
#include <memory>

namespace terrier::planner {

common::hash_t ProjectionPlanNode::Hash() const {
  auto type = GetPlanNodeType();
  common::hash_t hash = common::HashUtil::Hash(&type);

  // TODO(Gus,Wen): Hash output schema

  return common::HashUtil::CombineHashes(hash, AbstractPlanNode::Hash());
}

bool ProjectionPlanNode::operator==(const AbstractPlanNode &rhs) const { return AbstractPlanNode::operator==(rhs); }

nlohmann::json ProjectionPlanNode::ToJson() const {
  nlohmann::json j = AbstractPlanNode::ToJson();
  return j;
}

void ProjectionPlanNode::FromJson(const nlohmann::json &j) { AbstractPlanNode::FromJson(j); }

}  // namespace terrier::planner
