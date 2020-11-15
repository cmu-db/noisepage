#include "planner/plannodes/abstract_scan_plan_node.h"

#include <memory>
#include <utility>
#include <vector>

#include "catalog/catalog_defs.h"
#include "common/json.h"
#include "planner/plannodes/output_schema.h"

namespace noisepage::planner {

AbstractScanPlanNode::AbstractScanPlanNode(std::vector<std::unique_ptr<AbstractPlanNode>> &&children,
                                           std::unique_ptr<OutputSchema> output_schema,
                                           common::ManagedPointer<parser::AbstractExpression> predicate,
                                           bool is_for_update, catalog::db_oid_t database_oid, uint32_t scan_limit,
                                           bool scan_has_limit, uint32_t scan_offset, bool scan_has_offset)
    : AbstractPlanNode(std::move(children), std::move(output_schema)),
      scan_predicate_(predicate),
      is_for_update_(is_for_update),
      database_oid_(database_oid),
      scan_limit_(scan_limit),
      scan_has_limit_(scan_has_limit),
      scan_offset_(scan_offset),
      scan_has_offset_(scan_has_offset) {}

common::hash_t AbstractScanPlanNode::Hash() const {
  common::hash_t hash = AbstractPlanNode::Hash();

  // Database oid
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(database_oid_));

  // Predicate
  if (scan_predicate_ != nullptr) {
    hash = common::HashUtil::CombineHashes(hash, scan_predicate_->Hash());
  }

  // Is For Update Flag
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(is_for_update_));

  return hash;
}

bool AbstractScanPlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (!AbstractPlanNode::operator==(rhs)) return false;

  auto &other = dynamic_cast<const AbstractScanPlanNode &>(rhs);

  // Database Oid
  if (database_oid_ != other.database_oid_) return false;

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

  return true;
}

nlohmann::json AbstractScanPlanNode::ToJson() const {
  nlohmann::json j = AbstractPlanNode::ToJson();
  j["scan_predicate"] = scan_predicate_ == nullptr ? nlohmann::json(nullptr) : scan_predicate_->ToJson();
  j["is_for_update"] = is_for_update_;
  j["database_oid"] = database_oid_;
  return j;
}

std::vector<std::unique_ptr<parser::AbstractExpression>> AbstractScanPlanNode::FromJson(const nlohmann::json &j) {
  std::vector<std::unique_ptr<parser::AbstractExpression>> exprs;
  auto e1 = AbstractPlanNode::FromJson(j);
  exprs.insert(exprs.end(), std::make_move_iterator(e1.begin()), std::make_move_iterator(e1.end()));
  if (!j.at("scan_predicate").is_null()) {
    auto deserialized = parser::DeserializeExpression(j.at("scan_predicate"));
    scan_predicate_ = common::ManagedPointer(deserialized.result_);
    exprs.emplace_back(std::move(deserialized.result_));
    exprs.insert(exprs.end(), std::make_move_iterator(deserialized.non_owned_exprs_.begin()),
                 std::make_move_iterator(deserialized.non_owned_exprs_.end()));
  }
  is_for_update_ = j.at("is_for_update").get<bool>();
  database_oid_ = j.at("database_oid").get<catalog::db_oid_t>();
  return exprs;
}

}  // namespace noisepage::planner
