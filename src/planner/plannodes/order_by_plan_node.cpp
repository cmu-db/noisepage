#include "planner/plannodes/order_by_plan_node.h"
#include <utility>
#include <vector>

namespace terrier::planner {

common::hash_t OrderByPlanNode::Hash() const {
  auto type = GetPlanNodeType();
  common::hash_t hash = common::HashUtil::Hash(&type);

  for (const auto &sort_key : GetSortKeys()) {
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&sort_key.first));
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&sort_key.second));
  }

  hash = common::HashUtil::CombineHashes(hash, GetOutputSchema()->Hash());

  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&has_limit_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&limit_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&offset_));

  return common::HashUtil::CombineHashes(hash, AbstractPlanNode::Hash());
}

bool OrderByPlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (GetPlanNodeType() != rhs.GetPlanNodeType()) {
    return false;
  }

  auto &other = static_cast<const OrderByPlanNode &>(rhs);

  // Sort Keys
  if (GetSortKeys() != other.GetSortKeys()) {
    return false;
  }

  // Limit/Offset
  if (HasLimit() != other.HasLimit()) return false;
  if (HasLimit() && (GetOffset() != other.GetOffset() || GetLimit() != other.GetLimit())) return false;

  return AbstractPlanNode::operator==(rhs);
}

nlohmann::json OrderByPlanNode::ToJson() const {
  nlohmann::json j = AbstractPlanNode::ToJson();
  j["sort_keys"] = sort_keys_;
  j["has_limit"] = has_limit_;
  j["limit"] = limit_;
  j["offset"] = offset_;
  return j;
}

void OrderByPlanNode::FromJson(const nlohmann::json &j) {
  AbstractPlanNode::FromJson(j);
  sort_keys_ = j.at("sort_keys").get<std::vector<std::pair<catalog::col_oid_t, OrderByOrderingType>>>();
  has_limit_ = j.at("has_limit").get<bool>();
  limit_ = j.at("limit").get<size_t>();
  offset_ = j.at("offset").get<size_t>();
}

}  // namespace terrier::planner
