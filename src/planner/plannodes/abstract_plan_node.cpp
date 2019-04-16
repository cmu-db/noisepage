#include "planner/plannodes/abstract_plan_node.h"
#include <memory>
#include <vector>
#include "planner/plannodes/limit_plan_node.h"
#include "planner/plannodes/seq_scan_plan_node.h"

namespace terrier::planner {

nlohmann::json AbstractPlanNode::ToJson() const {
  nlohmann::json j;

  // TODO(Gus, Wen): Serialize output schema

  // Serialize Metadata
  j["PlanNodeType"] = GetPlanNodeType();
  j["children"] = children_;

  return j;
}

void AbstractPlanNode::FromJson(const nlohmann::json &j) {
  // TODO(Gus,Wen): Deserialize output schema
  // Deserialize children
  auto children_json = j.at("children").get<std::vector<nlohmann::json>>();
  for (const auto &child_json : children_json) {
    children_.push_back(DeserializePlanNode(child_json));
  }
}

std::shared_ptr<AbstractPlanNode> DeserializePlanNode(const nlohmann::json &json) {
  std::shared_ptr<AbstractPlanNode> plan_node;

  auto plan_type = json["PlanNodeType"].get<PlanNodeType>();
  switch (plan_type) {
    case PlanNodeType::LIMIT: {
      // Not sure if this is the best way to do this
      plan_node = std::make_unique<LimitPlanNode>();
      break;
    }

    default:
      throw std::runtime_error("Unknown plan node type during deserialization");
  }

  plan_node->FromJson(json);
  return plan_node;
}

}  // namespace terrier::planner
