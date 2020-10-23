#include "planner/plannodes/drop_index_plan_node.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/json.h"

namespace noisepage::planner {

common::hash_t DropIndexPlanNode::Hash() const {
  common::hash_t hash = AbstractPlanNode::Hash();

  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(index_oid_));

  return hash;
}

bool DropIndexPlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (!AbstractPlanNode::operator==(rhs)) return false;

  auto &other = dynamic_cast<const DropIndexPlanNode &>(rhs);

  return index_oid_ == other.index_oid_;
}

nlohmann::json DropIndexPlanNode::ToJson() const {
  nlohmann::json j = AbstractPlanNode::ToJson();
  j["index_oid"] = index_oid_;
  return j;
}

std::vector<std::unique_ptr<parser::AbstractExpression>> DropIndexPlanNode::FromJson(const nlohmann::json &j) {
  std::vector<std::unique_ptr<parser::AbstractExpression>> exprs;
  auto e1 = AbstractPlanNode::FromJson(j);
  exprs.insert(exprs.end(), std::make_move_iterator(e1.begin()), std::make_move_iterator(e1.end()));
  index_oid_ = j.at("index_oid").get<catalog::index_oid_t>();
  return exprs;
}

DEFINE_JSON_BODY_DECLARATIONS(DropIndexPlanNode);

}  // namespace noisepage::planner
