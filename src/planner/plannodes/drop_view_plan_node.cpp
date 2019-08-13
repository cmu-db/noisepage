#include "planner/plannodes/drop_view_plan_node.h"
#include <string>
#include <utility>

namespace terrier::planner {
common::hash_t DropViewPlanNode::Hash() const {
  common::hash_t hash = AbstractPlanNode::Hash();

  // Hash databse_oid
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(database_oid_));

  // Hash namespace oid
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(namespace_oid_));

  // Hash view_oid
  auto view_oid = GetViewOid();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(!view_oid));

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

  // Namespace OID
  if (namespace_oid_ != other.namespace_oid_) return false;

  // View OID
  if (GetViewOid() != other.GetViewOid()) return false;

  // If exists
  if (IsIfExists() != other.IsIfExists()) return false;

  return true;
}

nlohmann::json DropViewPlanNode::ToJson() const {
  nlohmann::json j = AbstractPlanNode::ToJson();
  j["database_oid"] = database_oid_;
  j["namespace_oid"] = namespace_oid_;
  j["view_oid"] = view_oid_;
  j["if_exists"] = if_exists_;
  return j;
}

void DropViewPlanNode::FromJson(const nlohmann::json &j) {
  AbstractPlanNode::FromJson(j);
  database_oid_ = j.at("database_oid").get<catalog::db_oid_t>();
  namespace_oid_ = j.at("namespace_oid").get<catalog::namespace_oid_t>();
  view_oid_ = j.at("view_oid").get<catalog::view_oid_t>();
  if_exists_ = j.at("if_exists").get<bool>();
}

}  // namespace terrier::planner
