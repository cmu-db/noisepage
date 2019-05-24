#include "planner/plannodes/insert_plan_node.h"
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include "parser/expression/constant_value_expression.h"
#include "storage/sql_table.h"
#include "type/transient_value_factory.h"

namespace terrier::planner {
common::hash_t InsertPlanNode::Hash() const {
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

  // Hash parameter_info
  for (const auto pair : parameter_info_) {
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(pair.first));
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(pair.second));
  }

  // Values
  for (uint32_t i = 0; i < GetBulkInsertCount(); i++) {
    for (const auto &value : GetValues(i)) {
      hash = common::HashUtil::CombineHashes(hash, value.Hash());
    }
  }

  return common::HashUtil::CombineHashes(hash, AbstractPlanNode::Hash());
}

bool InsertPlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (GetPlanNodeType() != rhs.GetPlanNodeType()) return false;

  auto &other = dynamic_cast<const InsertPlanNode &>(rhs);

  // Database OID
  if (GetDatabaseOid() != other.GetDatabaseOid()) return false;

  // Namespace OID
  if (GetNamespaceOid() != other.GetNamespaceOid()) return false;

  // Target table OID
  if (GetTableOid() != other.GetTableOid()) return false;

  // Bulk insert count
  if (GetBulkInsertCount() != other.GetBulkInsertCount()) return false;

  // Values
  for (uint32_t i = 0; i < GetBulkInsertCount(); i++) {
    if (GetValues(i) != other.GetValues(i)) return false;
  }

  // Parameter info
  if (GetParameterInfo() != other.GetParameterInfo()) return false;

  return AbstractPlanNode::operator==(rhs);
}

nlohmann::json InsertPlanNode::ToJson() const {
  nlohmann::json j = AbstractPlanNode::ToJson();
  j["database_oid"] = database_oid_;
  j["namespace_oid"] = namespace_oid_;
  j["table_oid"] = table_oid_;
  j["values"] = values_;
  j["parameter_info"] = parameter_info_;
  return j;
}

void InsertPlanNode::FromJson(const nlohmann::json &j) {
  AbstractPlanNode::FromJson(j);
  database_oid_ = j.at("database_oid").get<catalog::db_oid_t>();
  namespace_oid_ = j.at("namespace_oid").get<catalog::namespace_oid_t>();
  table_oid_ = j.at("table_oid").get<catalog::table_oid_t>();
  values_ = j.at("values").get<std::vector<std::vector<type::TransientValue>>>();
  parameter_info_ = j.at("parameter_info").get<std::unordered_map<uint32_t, catalog::col_oid_t>>();
}

}  // namespace terrier::planner
