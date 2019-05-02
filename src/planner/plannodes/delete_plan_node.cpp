#include "planner/plannodes/delete_plan_node.h"
#include <memory>
#include <utility>

namespace terrier::planner {

// TODO(Gus,Wen) Add SetParameters

common::hash_t DeletePlanNode::Hash() const {
  auto type = GetPlanNodeType();
  common::hash_t hash = common::HashUtil::Hash(&type);

  // Hash database_oid
  auto database_oid = GetDatabaseOid();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&database_oid));

  // Hash namespace oid
  auto namespace_oid = GetNamespaceOid();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&namespace_oid));

  // Hash table_oid
  auto table_oid = GetTableOid();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&table_oid));

  // Hash delete_condition
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(delete_condition_->Hash()));

  return common::HashUtil::CombineHashes(hash, AbstractPlanNode::Hash());
}

bool DeletePlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (GetPlanNodeType() != rhs.GetPlanNodeType()) return false;

  auto &other = dynamic_cast<const DeletePlanNode &>(rhs);

  // Database OID
  if (GetDatabaseOid() != other.GetDatabaseOid()) return false;

  // Namespace OID
  if (GetNamespaceOid() != other.GetNamespaceOid()) return false;

  // Table OID
  if (GetTableOid() != other.GetTableOid()) return false;

  // Delete condition
  if (*GetDeleteCondition() != *other.GetDeleteCondition()) return false;

  return AbstractPlanNode::operator==(rhs);
}

nlohmann::json DeletePlanNode::ToJson() const {
  nlohmann::json j = AbstractPlanNode::ToJson();
  j["database_oid"] = database_oid_;
  j["namespace_oid"] = namespace_oid_;
  j["table_oid"] = table_oid_;
  j["delete_condition"] = delete_condition_;
  return j;
}

void DeletePlanNode::FromJson(const nlohmann::json &j) {
  AbstractPlanNode::FromJson(j);
  database_oid_ = j.at("database_oid").get<catalog::db_oid_t>();
  namespace_oid_ = j.at("namespace_oid").get<catalog::namespace_oid_t>();
  table_oid_ = j.at("table_oid").get<catalog::table_oid_t>();
  if (!j.at("delete_condition").is_null()) {
    delete_condition_ = parser::DeserializeExpression(j.at("delete_condition"));
  }
}

}  // namespace terrier::planner
