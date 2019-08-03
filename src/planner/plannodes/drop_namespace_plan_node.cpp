#include "planner/plannodes/drop_namespace_plan_node.h"
#include <string>
#include <utility>

namespace terrier::planner {

common::hash_t DropNamespacePlanNode::Hash() const {
  common::hash_t hash = AbstractPlanNode::Hash();

  // Hash databse_oid
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(database_oid_));

  // Hash namespace_oid
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(namespace_oid_));

  // Hash if_exists_
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(if_exists_));

  return hash;
}

bool DropNamespacePlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (!AbstractPlanNode::operator==(rhs)) return false;

  auto &other = dynamic_cast<const DropNamespacePlanNode &>(rhs);

  // Database OID
  if (database_oid_ != other.database_oid_) return false;

  // Namespace OID
  if (namespace_oid_ != other.namespace_oid_) return false;

  // If exists
  if (if_exists_ != other.if_exists_) return false;

  return true;
}

nlohmann::json DropNamespacePlanNode::ToJson() const {
  nlohmann::json j = AbstractPlanNode::ToJson();
  j["database_oid"] = database_oid_;
  j["namespace_oid"] = namespace_oid_;
  j["if_exists"] = if_exists_;
  return j;
}

void DropNamespacePlanNode::FromJson(const nlohmann::json &j) {
  AbstractPlanNode::FromJson(j);
  database_oid_ = j.at("database_oid").get<catalog::db_oid_t>();
  namespace_oid_ = j.at("namespace_oid").get<catalog::namespace_oid_t>();
  if_exists_ = j.at("if_exists").get<bool>();
}

}  // namespace terrier::planner
