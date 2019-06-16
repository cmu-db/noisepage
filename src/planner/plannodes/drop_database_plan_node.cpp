#include "planner/plannodes/drop_database_plan_node.h"
#include <string>
#include <utility>

namespace terrier::planner {

common::hash_t DropDatabasePlanNode::Hash() const {
  common::hash_t hash = AbstractPlanNode::Hash();

  // Database Oid
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(database_oid_));

  // If Exists
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(if_exists_));

  return hash;
}

bool DropDatabasePlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (GetPlanNodeType() != rhs.GetPlanNodeType()) return false;

  auto &other = dynamic_cast<const DropDatabasePlanNode &>(rhs);

  // Database OID
  if (database_oid_ != other.database_oid_) return false;

  // If exists
  if (if_exists_ != other.if_exists_) return false;

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
