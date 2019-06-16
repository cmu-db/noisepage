#include "planner/plannodes/limit_plan_node.h"
#include "common/hash_util.h"
#include "planner/plannodes/hash_plan_node.h"

namespace terrier::planner {

common::hash_t LimitPlanNode::Hash() const {
  common::hash_t hash = AbstractPlanNode::Hash();

  // Limit
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(limit_));

  // Offset
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(offset_));

  return hash;
}

bool LimitPlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (GetPlanNodeType() != rhs.GetPlanNodeType()) return false;

  auto &other = static_cast<const LimitPlanNode &>(rhs);

  // Limit
  if (limit_ != other.limit_) return false;

  // Offset
  if (offset_ != other.offset_) return false;

  return AbstractPlanNode::operator==(rhs);
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
