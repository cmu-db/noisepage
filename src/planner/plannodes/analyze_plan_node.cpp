#include "planner/plannodes/analyze_plan_node.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "catalog/catalog_defs.h"
#include "common/json.h"
#include "planner/plannodes/output_schema.h"

namespace noisepage::planner {

std::unique_ptr<AnalyzePlanNode> AnalyzePlanNode::Builder::Build() {
  return std::unique_ptr<AnalyzePlanNode>(new AnalyzePlanNode(std::move(children_), std::move(output_schema_),
                                                              database_oid_, table_oid_, std::move(column_oids_)));
}

AnalyzePlanNode::AnalyzePlanNode(std::vector<std::unique_ptr<AbstractPlanNode>> &&children,
                                 std::unique_ptr<OutputSchema> output_schema, catalog::db_oid_t database_oid,
                                 catalog::table_oid_t table_oid, std::vector<catalog::col_oid_t> &&column_oids)
    : AbstractPlanNode(std::move(children), std::move(output_schema)),
      database_oid_(database_oid),
      table_oid_(table_oid),
      column_oids_(std::move(column_oids)) {}

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
}  // namespace noisepage::planner
