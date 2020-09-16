#include "planner/plannodes/analyze_plan_node.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "catalog/catalog_defs.h"
#include "common/json.h"

namespace terrier::planner {

common::hash_t AnalyzePlanNode::Hash() const {
  common::hash_t hash = AbstractPlanNode::Hash();

  // Hash database_oid
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(database_oid_));

  // Hash table_oid
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(table_oid_));

  // Hash column_oids
  hash = common::HashUtil::CombineHashInRange(hash, column_oids_.begin(), column_oids_.end());

  return hash;
}

bool AnalyzePlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (!AbstractPlanNode::operator==(rhs)) return false;

  auto &other = dynamic_cast<const AnalyzePlanNode &>(rhs);

  // Database OID
  if (database_oid_ != other.database_oid_) return false;

  // Target table OID
  if (table_oid_ != other.table_oid_) return false;

  // Column Oids
  if (column_oids_ != other.column_oids_) return false;

  return true;
}

nlohmann::json AnalyzePlanNode::ToJson() const {
  nlohmann::json j = AbstractPlanNode::ToJson();
  j["database_oid"] = database_oid_;
  j["table_oid"] = table_oid_;
  j["column_oids"] = column_oids_;
  return j;
}

std::vector<std::unique_ptr<parser::AbstractExpression>> AnalyzePlanNode::FromJson(const nlohmann::json &j) {
  std::vector<std::unique_ptr<parser::AbstractExpression>> exprs;
  auto e1 = AbstractPlanNode::FromJson(j);
  exprs.insert(exprs.end(), std::make_move_iterator(e1.begin()), std::make_move_iterator(e1.end()));
  database_oid_ = j.at("database_oid").get<catalog::db_oid_t>();
  table_oid_ = j.at("table_oid").get<catalog::table_oid_t>();
  column_oids_ = j.at("column_oids").get<std::vector<catalog::col_oid_t>>();
  return exprs;
}
}  // namespace terrier::planner
