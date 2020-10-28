#include "planner/plannodes/drop_trigger_plan_node.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/json.h"

namespace noisepage::planner {
common::hash_t DropTriggerPlanNode::Hash() const {
  common::hash_t hash = AbstractPlanNode::Hash();

  // Hash database_oid
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(database_oid_));

  // Hash namespace oid
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(namespace_oid_));

  // Hash trigger_oid
  auto trigger_oid = GetTriggerOid();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(trigger_oid));

  // Hash if_exists_
  auto if_exist = IsIfExists();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(if_exist));

  return hash;
}

bool DropTriggerPlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (!AbstractPlanNode::operator==(rhs)) return false;

  auto &other = dynamic_cast<const DropTriggerPlanNode &>(rhs);

  // Database OID
  if (database_oid_ != other.database_oid_) return false;

  // Namespace OID
  if (namespace_oid_ != other.namespace_oid_) return false;

  // Trigger OID
  if (GetTriggerOid() != other.GetTriggerOid()) return false;

  // If exists
  if (IsIfExists() != other.IsIfExists()) return false;

  return true;
}

nlohmann::json DropTriggerPlanNode::ToJson() const {
  nlohmann::json j = AbstractPlanNode::ToJson();
  j["database_oid"] = database_oid_;
  j["namespace_oid"] = namespace_oid_;
  j["trigger_oid"] = trigger_oid_;
  j["if_exists"] = if_exists_;
  return j;
}

std::vector<std::unique_ptr<parser::AbstractExpression>> DropTriggerPlanNode::FromJson(const nlohmann::json &j) {
  std::vector<std::unique_ptr<parser::AbstractExpression>> exprs;
  auto e1 = AbstractPlanNode::FromJson(j);
  exprs.insert(exprs.end(), std::make_move_iterator(e1.begin()), std::make_move_iterator(e1.end()));
  database_oid_ = j.at("database_oid").get<catalog::db_oid_t>();
  namespace_oid_ = j.at("namespace_oid").get<catalog::namespace_oid_t>();
  trigger_oid_ = j.at("trigger_oid").get<catalog::trigger_oid_t>();
  if_exists_ = j.at("if_exists").get<bool>();
  return exprs;
}

DEFINE_JSON_BODY_DECLARATIONS(DropTriggerPlanNode);

}  // namespace noisepage::planner
