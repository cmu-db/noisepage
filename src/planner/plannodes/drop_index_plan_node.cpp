#include "planner/plannodes/drop_index_plan_node.h"
#include <string>
#include <utility>

namespace terrier::planner {

common::hash_t DropIndexPlanNode::Hash() const {
  common::hash_t hash = AbstractPlanNode::Hash();

  // Hash databse_oid
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(database_oid_));

  // Hash namespace oid
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(namespace_oid_));

  // Hash index_oid
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(index_oid_));

  // Hash if_exists_
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(if_exists_));

  return hash;
}

bool DropIndexPlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (GetPlanNodeType() != rhs.GetPlanNodeType()) return false;

  auto &other = dynamic_cast<const DropIndexPlanNode &>(rhs);

  // Database OID
  if (GetDatabaseOid() != other.GetDatabaseOid()) return false;

  // Namespace OID
  if (GetNamespaceOid() != other.GetNamespaceOid()) return false;

  // Index OID
  if (GetIndexOid() != other.GetIndexOid()) return false;

  // If exists
  if (IsIfExists() != other.IsIfExists()) return false;

  return AbstractPlanNode::operator==(rhs);
}

nlohmann::json DropIndexPlanNode::ToJson() const {
  nlohmann::json j = AbstractPlanNode::ToJson();
  j["database_oid"] = database_oid_;
  j["namespace_oid"] = namespace_oid_;
  j["index_oid"] = index_oid_;
  j["if_exists"] = if_exists_;
  return j;
}

void DropIndexPlanNode::FromJson(const nlohmann::json &j) {
  AbstractPlanNode::FromJson(j);
  database_oid_ = j.at("database_oid").get<catalog::db_oid_t>();
  namespace_oid_ = j.at("namespace_oid").get<catalog::namespace_oid_t>();
  index_oid_ = j.at("index_oid").get<catalog::index_oid_t>();
  if_exists_ = j.at("if_exists").get<bool>();
}

}  // namespace terrier::planner
