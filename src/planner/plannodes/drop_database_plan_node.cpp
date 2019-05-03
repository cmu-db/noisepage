#include "planner/plannodes/drop_database_plan_node.h"
#include <string>
#include <utility>

namespace terrier::planner {
common::hash_t DropDatabasePlanNode::Hash() const {
  auto type = GetPlanNodeType();
  common::hash_t hash = common::HashUtil::Hash(&type);

  // Hash database oid
  auto database_oid = GetDatabaseOid();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&database_oid));

  // Hash if_exists_
  auto if_exist = IsIfExists();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&if_exist));

  return common::HashUtil::CombineHashes(hash, AbstractPlanNode::Hash());
}

bool DropDatabasePlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (GetPlanNodeType() != rhs.GetPlanNodeType()) return false;

  auto &other = dynamic_cast<const DropDatabasePlanNode &>(rhs);

  // Database OID
  if (GetDatabaseOid() != other.GetDatabaseOid()) return false;

  // If exists
  if (IsIfExists() != other.IsIfExists()) return false;

  return AbstractPlanNode::operator==(rhs);
}

nlohmann::json DropDatabasePlanNode::ToJson() const {
  nlohmann::json j = AbstractPlanNode::ToJson();
  j["database_oid"] = database_oid_;
  j["if_exists"] = if_exists_;
  return j;
}

void DropDatabasePlanNode::FromJson(const nlohmann::json &j) {
  AbstractPlanNode::FromJson(j);
  database_oid_ = j.at("database_oid").get<catalog::db_oid_t>();
  if_exists_ = j.at("if_exists").get<bool>();
}

}  // namespace terrier::planner
