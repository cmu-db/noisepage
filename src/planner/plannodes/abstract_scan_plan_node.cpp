#include "planner/plannodes/abstract_scan_plan_node.h"
#include <catalog/catalog_defs.h>

namespace terrier::planner {

common::hash_t AbstractScanPlanNode::Hash() const {
  common::hash_t hash = AbstractPlanNode::Hash();

  // Database oid
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(database_oid_));

  // Namespace oid
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(namespace_oid_));

  // Predicate
  if (scan_predicate_ != nullptr) {
    hash = common::HashUtil::CombineHashes(hash, scan_predicate_->Hash());
  }

  // Is For Update Flag
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(is_for_update_));

  // Is Parallel Flag
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(is_parallel_));

  return hash;
}

bool AbstractScanPlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (!AbstractPlanNode::operator==(rhs)) return false;

  auto &other = dynamic_cast<const AbstractScanPlanNode &>(rhs);

  // Database Oid
  if (database_oid_ != other.database_oid_) return false;

  // Namespace Oid
  if (namespace_oid_ != other.namespace_oid_) return false;

  // Predicate
  if ((scan_predicate_ == nullptr && other.scan_predicate_ != nullptr) ||
      (scan_predicate_ != nullptr && other.scan_predicate_ == nullptr)) {
    return false;
  }
  if (scan_predicate_ != nullptr && *scan_predicate_ != *other.scan_predicate_) {
    return false;
  }

  // Is For Update Flag
  if (is_for_update_ != other.is_for_update_) return false;

  // Is Parallel Flag
  if (is_parallel_ != other.is_parallel_) return false;

  return true;
}

nlohmann::json AbstractScanPlanNode::ToJson() const {
  nlohmann::json j = AbstractPlanNode::ToJson();
  j["scan_predicate"] = scan_predicate_;
  j["is_for_update"] = is_for_update_;
  j["is_parallel"] = is_parallel_;
  j["database_oid"] = database_oid_;
  j["namespace_oid"] = namespace_oid_;
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
  namespace_oid_ = j.at("namespace_oid").get<catalog::namespace_oid_t>();
}

}  // namespace terrier::planner
