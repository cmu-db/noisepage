#include "planner/plannodes/drop_view_plan_node.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/json.h"
#include "planner/plannodes/output_schema.h"

namespace noisepage::planner {

std::unique_ptr<DropViewPlanNode> DropViewPlanNode::Builder::Build() {
  return std::unique_ptr<DropViewPlanNode>(new DropViewPlanNode(std::move(children_), std::move(output_schema_),
                                                                database_oid_, view_oid_, if_exists_, plan_node_id_));
}

DropViewPlanNode::DropViewPlanNode(std::vector<std::unique_ptr<AbstractPlanNode>> &&children,
                                   std::unique_ptr<OutputSchema> output_schema, catalog::db_oid_t database_oid,
                                   catalog::view_oid_t view_oid, bool if_exists, plan_node_id_t plan_node_id)
    : AbstractPlanNode(std::move(children), std::move(output_schema), plan_node_id),
      database_oid_(database_oid),
      view_oid_(view_oid),
      if_exists_(if_exists) {}

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
