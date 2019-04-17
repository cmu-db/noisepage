#include "planner/plannodes/abstract_scan_plan_node.h"

namespace terrier::planner {

bool AbstractScanPlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (GetPlanNodeType() != rhs.GetPlanNodeType()) {
    return false;
  }

  // Check predicate
  auto &other = dynamic_cast<const AbstractScanPlanNode &>(rhs);

  auto &pred = GetScanPredicate();
  auto &other_pred = other.GetScanPredicate();
  if ((pred == nullptr && other_pred != nullptr) || (pred != nullptr && other_pred == nullptr)) {
    return false;
  }
  if (pred != nullptr && *pred != *other_pred) {
    return false;
  }

  return IsForUpdate() == other.IsForUpdate() && IsParallel() == other.IsParallel() &&
         GetDatabaseOid() == other.GetDatabaseOid();
}

common::hash_t AbstractScanPlanNode::Hash() const {
  auto type = GetPlanNodeType();
  common::hash_t hash = common::HashUtil::Hash(&type);

  // Hash predicate
  if (GetScanPredicate() != nullptr) {
    hash = common::HashUtil::CombineHashes(hash, GetScanPredicate()->Hash());
  }

  // Hash update flag
  auto is_for_update = IsForUpdate();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&is_for_update));

  // Hash parallel flag
  auto is_parallel = IsParallel();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&is_parallel));

  // Hash database oid
  auto database_oid = GetDatabaseOid();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&database_oid));

  return hash;
}

nlohmann::json AbstractScanPlanNode::ToJson() const {
  nlohmann::json j = AbstractPlanNode::ToJson();
  j["scan_predicate"] = scan_predicate_;
  j["is_for_update"] = is_for_update_;
  j["is_parallel"] = is_parallel_;
  j["database_oid"] = database_oid_;
  return j;
}

void AbstractScanPlanNode::FromJson(const nlohmann::json &j) {
  AbstractPlanNode::FromJson(j);
  if (!j.at("scan_predicate").is_null()) {
    scan_predicate_ = parser::DeserializeExpression(j.at("scan_predicate"));
  }
  is_for_update_ = j.at("is_for_update").get<bool>();
  is_parallel_ = j.at("is_parallel").get<bool>();
  database_oid_ = j.at("database_oid").get<catalog::db_oid_t>();
}

}  // namespace terrier::planner
