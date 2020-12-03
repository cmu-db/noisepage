#include "planner/plannodes/projection_plan_node.h"

#include <memory>
#include <vector>

#include "common/json.h"
#include "planner/plannodes/output_schema.h"

namespace noisepage::planner {

std::unique_ptr<ProjectionPlanNode> ProjectionPlanNode::Builder::Build() {
  return std::unique_ptr<ProjectionPlanNode>(
      new ProjectionPlanNode(std::move(children_), std::move(output_schema_), plan_node_id_));
}

ProjectionPlanNode::ProjectionPlanNode(std::vector<std::unique_ptr<AbstractPlanNode>> &&children,
                                       std::unique_ptr<OutputSchema> output_schema, plan_node_id_t plan_node_id)
    : AbstractPlanNode(std::move(children), std::move(output_schema), plan_node_id) {}

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

std::vector<std::unique_ptr<parser::AbstractExpression>> ProjectionPlanNode::FromJson(const nlohmann::json &j) {
  std::vector<std::unique_ptr<parser::AbstractExpression>> exprs;
  auto e1 = AbstractPlanNode::FromJson(j);
  exprs.insert(exprs.end(), std::make_move_iterator(e1.begin()), std::make_move_iterator(e1.end()));
  return exprs;
}

DEFINE_JSON_BODY_DECLARATIONS(ProjectionPlanNode);

}  // namespace noisepage::planner
