#include "planner/plannodes/analyze_plan_node.h"
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "catalog/catalog_defs.h"

namespace terrier::planner {

common::hash_t AnalyzePlanNode::Hash() const {
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

  // Hash column_names
  for (const auto column_oid : column_oids_) {
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&column_oid));
  }

  return common::HashUtil::CombineHashes(hash, AbstractPlanNode::Hash());
}

bool AnalyzePlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (GetPlanNodeType() != rhs.GetPlanNodeType()) return false;

  auto &other = dynamic_cast<const AnalyzePlanNode &>(rhs);

  // Database OID
  if (GetDatabaseOid() != other.GetDatabaseOid()) return false;

  // Namespace OID
  if (GetNamespaceOid() != other.GetNamespaceOid()) return false;

  // Target table OID
  if (GetTableOid() != other.GetTableOid()) return false;

  // Column names
  const auto &column_oids = GetColumnOids();
  const auto &other_column_oids = other.GetColumnOids();
  if (column_oids.size() != other_column_oids.size()) return false;

  for (size_t i = 0; i < column_oids.size(); i++) {
    if (column_oids[i] != other_column_oids[i]) {
      return false;
    }
  }

  return AbstractPlanNode::operator==(rhs);
}

nlohmann::json AnalyzePlanNode::ToJson() const {
  nlohmann::json j = AbstractPlanNode::ToJson();
  j["database_oid"] = database_oid_;
  j["namespace_oid"] = namespace_oid_;
  j["table_oid"] = table_oid_;
  j["column_oids"] = column_oids_;
  return j;
}

void AnalyzePlanNode::FromJson(const nlohmann::json &j) {
  AbstractPlanNode::FromJson(j);
  database_oid_ = j.at("database_oid").get<catalog::db_oid_t>();
  namespace_oid_ = j.at("namespace_oid").get<catalog::namespace_oid_t>();
  table_oid_ = j.at("table_oid").get<catalog::table_oid_t>();
  column_oids_ = j.at("column_oids").get<std::vector<catalog::col_oid_t>>();
}
}  // namespace terrier::planner
