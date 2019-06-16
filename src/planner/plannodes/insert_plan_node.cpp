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
  common::hash_t hash = AbstractPlanNode::Hash();

  // Hash database_oid
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(database_oid_));

  // Hash namespace oid
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(namespace_oid_));

  // Hash table_oid
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(table_oid_));

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

  return hash;
}

bool InsertPlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (GetPlanNodeType() != rhs.GetPlanNodeType()) return false;

  auto &other = dynamic_cast<const InsertPlanNode &>(rhs);

  // Database OID
  if (database_oid_ != other.database_oid_) return false;

  // Namespace OID
  if (namespace_oid_ != other.namespace_oid_) return false;

  // Target table OID
  if (table_oid_ != other.table_oid_) return false;

  // Values
  if (values_ != other.values_) return false;

  // Parameter info
  if (parameter_info_ != other.parameter_info_) return false;

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
