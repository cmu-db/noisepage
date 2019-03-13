#include "plan_node/limit_plan_node.h"
#include "common/hash_util.h"
#include "plan_node/hash_plan_node.h"

namespace terrier::plan_node {

inline common::hash_t LimitPlanNode::Hash() const {
  auto type = GetPlanNodeType();
  common::hash_t hash = common::HashUtil::Hash(&type);
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&limit_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&offset_));

  // TODO(Gus,Wen): Hash output schema

  return common::HashUtil::CombineHashes(hash, AbstractPlanNode::Hash());
}

inline bool LimitPlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (GetPlanNodeType() != rhs.GetPlanNodeType()) {
    return false;
  }
  auto &other = static_cast<const plan_node::LimitPlanNode &>(rhs);
  return (limit_ == other.limit_ && offset_ == other.offset_ && AbstractPlanNode::operator==(rhs));
}

nlohmann::json LimitPlanNode::ToJson() const {
  nlohmann::json output;
  output["Limit"] = limit_;
  output["Offset"] = offset_;

  // Serialize metadata from base class
  auto abstract_plan_json = AbstractPlanNode::ToJson();
  output.update(abstract_plan_json);

  return output;
}

void LimitPlanNode::FromJson(const nlohmann::json &json) {
  TERRIER_ASSERT(GetPlanNodeType() == json["PlanNodeType"].get<PlanNodeType>(), "Mismatching plan node types");
  limit_ = json["Limit"].get<size_t>();
  offset_ = json["Offset"].get<size_t>();

  AbstractPlanNode::FromJson(json);
}

}  // namespace terrier::plan_node
