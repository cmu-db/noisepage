#include "planner/plannodes/create_namespace_plan_node.h"
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "parser/parser_defs.h"

namespace terrier::planner {

common::hash_t CreateNamespacePlanNode::Hash() const {
  common::hash_t hash = AbstractPlanNode::Hash();

  // Hash database_oid
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(database_oid_));

  // Hash namespace_name
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(namespace_name_));

  return hash;
}

bool CreateNamespacePlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (!AbstractPlanNode::operator==(rhs)) return false;

  auto &other = dynamic_cast<const CreateNamespacePlanNode &>(rhs);

  // Database OID
  if (database_oid_ != other.database_oid_) return false;

  // Schema name
  if (GetNamespaceName() != other.GetNamespaceName()) return false;

  return true;
}

nlohmann::json CreateNamespacePlanNode::ToJson() const {
  nlohmann::json j = AbstractPlanNode::ToJson();
  j["database_oid"] = database_oid_;
  j["namespace_name"] = namespace_name_;
  return j;
}

void CreateNamespacePlanNode::FromJson(const nlohmann::json &j) {
  AbstractPlanNode::FromJson(j);
  database_oid_ = j.at("database_oid").get<catalog::db_oid_t>();
  namespace_name_ = j.at("namespace_name").get<std::string>();
}

}  // namespace terrier::planner
