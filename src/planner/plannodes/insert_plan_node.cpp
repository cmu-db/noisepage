#include "planner/plannodes/insert_plan_node.h"
#include <memory>
#include <string>
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

  // Hash table_oid
  auto table_oid = GetTableOid();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&table_oid));

  // Hash parameter_info
  for (const auto parameter : parameter_info_) {
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(parameter));
  }

  // Hash bulk_insert_count
  auto bulk_insert_count = GetBulkInsertCount();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&bulk_insert_count));

  return common::HashUtil::CombineHashes(hash, AbstractPlanNode::Hash());
}

bool InsertPlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (GetPlanNodeType() != rhs.GetPlanNodeType()) return false;

  auto &other = dynamic_cast<const InsertPlanNode &>(rhs);

  // Database OID
  if (GetDatabaseOid() != other.GetDatabaseOid()) return false;

  // Target table OID
  if (GetTableOid() != other.GetTableOid()) return false;

  // TODO(Gus,Wen) compare values

  // Parameter info
  const auto &parameter_info = GetParameterInfo();
  const auto &other_parameter_info = other.GetParameterInfo();
  if (parameter_info.size() != other_parameter_info.size()) return false;

  for (size_t i = 0; i < parameter_info.size(); i++) {
    if (parameter_info[i] != other_parameter_info[i]) {
      return false;
    }
  }

  if (GetBulkInsertCount() != other.GetBulkInsertCount()) return false;

  return AbstractPlanNode::operator==(rhs);
}

}  // namespace terrier::planner
