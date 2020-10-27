#include "planner/plannodes/limit_plan_node.h"

#include <memory>
#include <vector>

#include "common/hash_util.h"
#include "common/json.h"

namespace noisepage::planner {

common::hash_t LimitPlanNode::Hash() const {
  common::hash_t hash = AbstractPlanNode::Hash();

  // Limit
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(limit_));

  // Offset
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(offset_));

  return hash;
}

bool LimitPlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (!AbstractPlanNode::operator==(rhs)) return false;

  auto &other = static_cast<const LimitPlanNode &>(rhs);

  // Limit
  if (limit_ != other.limit_) return false;

  // Offset
  if (offset_ != other.offset_) return false;

  return true;
}

nlohmann::json LimitPlanNode::ToJson() const {
  nlohmann::json j = AbstractPlanNode::ToJson();
  j["limit"] = limit_;
  j["offset"] = offset_;
  return j;
}

std::vector<std::unique_ptr<parser::AbstractExpression>> LimitPlanNode::FromJson(const nlohmann::json &j) {
  std::vector<std::unique_ptr<parser::AbstractExpression>> exprs;
  auto e1 = AbstractPlanNode::FromJson(j);
  exprs.insert(exprs.end(), std::make_move_iterator(e1.begin()), std::make_move_iterator(e1.end()));
  limit_ = j.at("limit").get<size_t>();
  offset_ = j.at("offset").get<size_t>();
  return exprs;
}

DEFINE_JSON_BODY_DECLARATIONS(LimitPlanNode);

}  // namespace noisepage::planner
