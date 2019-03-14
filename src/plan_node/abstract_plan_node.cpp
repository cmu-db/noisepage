#include "plan_node/abstract_plan_node.h"
#include "plan_node/limit_plan_node.h"
#include "plan_node/seq_scan_plan_node.h"

namespace terrier::plan_node {

nlohmann::json AbstractPlanNode::ToJson() const {
  nlohmann::json output;

  // TODO(Gus, Wen): Serialize output schema

  // Serialize Metadata
  output["PlanNodeType"] = GetPlanNodeType();
  output["EstimatedCardinality"] = GetEstimatedCardinality();

  // Serialize Children
  if (GetChildrenSize() > 0) {
    output["Children"] = {};

    for (auto &child : GetChildren()) {
      output["Children"].push_back(child->ToJson());
    }
  }

  return output;
}

void AbstractPlanNode::FromJson(const nlohmann::json &json) {
  // TODO(Gus,Wen): Deserialize output schema
  estimated_cardinality_ = json["EstimatedCardinality"].get<int>();
}

std::unique_ptr<AbstractPlanNode> DeserializePlanNode(const nlohmann::json &json) {
  std::unique_ptr<AbstractPlanNode> plan_node;

  auto plan_type = json["PlanNodeType"].get<PlanNodeType>();
  switch (plan_type) {
    case PlanNodeType::LIMIT: {
      // Not sure if this is the best way to do this
      // plan_node = std::make_unique<LimitPlanNode>(new LimitPlanNode());
      // plan_node->FromJson(json);
      break;
    }

    default:
      // This is 100% a hack, remove later
      TERRIER_ASSERT(false, "Unknown Plan Node Type during deserialization");
  }

  // Deserialize children
  if (json.find("Children") != json.end()) {
    auto children_json = json["Children"].get<std::vector<nlohmann::json>>();
    for (const auto &child_json : children_json) {
      plan_node->AddChild(DeserializePlanNode(child_json));
    }
  }

  return plan_node;
}

}  // namespace terrier::plan_node