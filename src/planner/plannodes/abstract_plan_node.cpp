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
  j["plan_node_type"] = GetPlanNodeType();
  j["children"] = children_;
  j["output_schema"] = output_schema_;

  return j;
}

void AbstractPlanNode::FromJson(const nlohmann::json &j) {
  // Deserialize children
  auto children_json = j.at("children").get<std::vector<nlohmann::json>>();
  for (const auto &child_json : children_json) {
    children_.push_back(DeserializePlanNode(child_json));
  }
  // Deserialize output schema
  if (!j.at("output_schema").is_null()) {
    output_schema_ = std::make_shared<OutputSchema>();
    output_schema_->FromJson(j.at("output_schema"));
  }
}

std::shared_ptr<AbstractPlanNode> DeserializePlanNode(const nlohmann::json &json) {
  std::shared_ptr<AbstractPlanNode> plan_node;

  auto plan_type = json.at("plan_node_type").get<PlanNodeType>();
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
