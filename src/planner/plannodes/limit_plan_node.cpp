#include "planner/plannodes/limit_plan_node.h"
#include "common/hash_util.h"
#include "planner/plannodes/hash_plan_node.h"

namespace terrier::planner {

common::hash_t LimitPlanNode::Hash() const {
  auto type = GetPlanNodeType();
  common::hash_t hash = common::HashUtil::Hash(&type);
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&limit_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&offset_));

  // TODO(Gus,Wen): Hash output schema

  return common::HashUtil::CombineHashes(hash, AbstractPlanNode::Hash());
}

bool LimitPlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (GetPlanNodeType() != rhs.GetPlanNodeType()) {
    return false;
  }
  auto &other = static_cast<const LimitPlanNode &>(rhs);
  return (limit_ == other.limit_ && offset_ == other.offset_ && AbstractPlanNode::operator==(rhs));
}

nlohmann::json LimitPlanNode::ToJson() const {
  nlohmann::json j = AbstractPlanNode::ToJson();
  j["limit"] = limit_;
  j["offset"] = offset_;
  return j;
}

void LimitPlanNode::FromJson(const nlohmann::json &j) {
  AbstractPlanNode::FromJson(j);
  limit_ = j.at("limit").get<size_t>();
  offset_ = j.at("offset").get<size_t>();
}

}  // namespace terrier::planner
