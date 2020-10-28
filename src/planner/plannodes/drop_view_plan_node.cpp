#include "planner/plannodes/drop_view_plan_node.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/json.h"

namespace noisepage::planner {
common::hash_t DropViewPlanNode::Hash() const {
  common::hash_t hash = AbstractPlanNode::Hash();

  // Hash databse_oid
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(database_oid_));

  // Hash view_oid
  auto view_oid = GetViewOid();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(view_oid.UnderlyingValue()));

  // Hash if_exists_
  auto if_exist = IsIfExists();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(!if_exist));

  return hash;
}

bool DropViewPlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (!AbstractPlanNode::operator==(rhs)) return false;

  auto &other = dynamic_cast<const DropViewPlanNode &>(rhs);

  // Database OID
  if (database_oid_ != other.database_oid_) return false;

  // View OID
  if (GetViewOid() != other.GetViewOid()) return false;

  // If exists
  if (IsIfExists() != other.IsIfExists()) return false;

  return true;
}

nlohmann::json DropViewPlanNode::ToJson() const {
  nlohmann::json j = AbstractPlanNode::ToJson();
  j["database_oid"] = database_oid_;
  j["view_oid"] = view_oid_;
  j["if_exists"] = if_exists_;
  return j;
}

std::vector<std::unique_ptr<parser::AbstractExpression>> DropViewPlanNode::FromJson(const nlohmann::json &j) {
  std::vector<std::unique_ptr<parser::AbstractExpression>> exprs;
  auto e1 = AbstractPlanNode::FromJson(j);
  exprs.insert(exprs.end(), std::make_move_iterator(e1.begin()), std::make_move_iterator(e1.end()));
  database_oid_ = j.at("database_oid").get<catalog::db_oid_t>();
  view_oid_ = j.at("view_oid").get<catalog::view_oid_t>();
  if_exists_ = j.at("if_exists").get<bool>();
  return exprs;
}

DEFINE_JSON_BODY_DECLARATIONS(DropViewPlanNode);

}  // namespace noisepage::planner
