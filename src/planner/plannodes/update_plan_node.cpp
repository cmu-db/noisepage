#include "planner/plannodes/update_plan_node.h"
#include <memory>
#include <utility>
#include "planner/plannodes/abstract_scan_plan_node.h"

namespace terrier::planner {

common::hash_t UpdatePlanNode::Hash() const {
  auto type = GetPlanNodeType();
  common::hash_t hash = common::HashUtil::Hash(&type);

  // Hash database_oid
  auto database_oid = GetDatabaseOid();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&database_oid));

  // Hash table_oid
  auto table_oid = GetTableOid();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&table_oid));

  // Hash update_primary_key
  auto is_update_primary_key = GetUpdatePrimaryKey();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&is_update_primary_key));

  return common::HashUtil::CombineHashes(hash, AbstractPlanNode::Hash());
}

bool UpdatePlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (GetPlanNodeType() != rhs.GetPlanNodeType()) return false;

  auto &other = static_cast<const UpdatePlanNode &>(rhs);

  // Database OID
  if (GetDatabaseOid() != other.GetDatabaseOid()) return false;

  // Table OID
  if (GetTableOid() != other.GetTableOid()) return false;

  // Update primary key
  if (GetUpdatePrimaryKey() != other.GetUpdatePrimaryKey()) return false;

  return AbstractPlanNode::operator==(rhs);
}

nlohmann::json UpdatePlanNode::ToJson() const {
  nlohmann::json j = AbstractPlanNode::ToJson();
  j["database_oid"] = database_oid_;
  j["table_oid"] = table_oid_;
  j["update_primary_key"] = update_primary_key_;
  return j;
}

void UpdatePlanNode::FromJson(const nlohmann::json &j) {
   AbstractPlanNode::FromJson(j);
   database_oid_ = j.at("database_oid").get<catalog::db_oid_t>();
   table_oid_ = j.at("table_oid").get<catalog::table_oid_t>();
   update_primary_key_ = j.at("update_primary_key").get<bool>();
}

}  // namespace terrier::planner
