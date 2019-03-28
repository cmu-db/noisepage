#include "plan_node/drop_plan_node.h"
#include <string>
#include <utility>

namespace terrier::plan_node {
common::hash_t DropPlanNode::Hash() const {
  auto type = GetPlanNodeType();
  common::hash_t hash = common::HashUtil::Hash(&type);

  // Hash drop_type
  auto drop_type = GetDropType();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&drop_type));

  // Hash table_name
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(GetTableName()));

  // Hash databse_name
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(GetDatabaseName()));

  // Hash schema_name
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(GetSchemaName()));

  // Hash trigger_name
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(GetTriggerName()));

  // Hash index_name
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(GetIndexName()));

  // Hash if_exists_
  auto if_exist = IsIfExists();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&if_exist));

  return common::HashUtil::CombineHashes(hash, AbstractPlanNode::Hash());
}

bool DropPlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (GetPlanNodeType() != rhs.GetPlanNodeType()) return false;

  auto &other = dynamic_cast<const DropPlanNode &>(rhs);

  // Drop type
  if (GetDropType() != other.GetDropType()) return false;

  // Table name
  if (GetTableName() != other.GetTableName()) return false;

  // Database name
  if (GetDatabaseName() != other.GetDatabaseName()) return false;

  // Schema name
  if (GetSchemaName() != other.GetSchemaName()) return false;

  // Trigger name
  if (GetTriggerName() != other.GetTriggerName()) return false;

  // Index name
  if (GetIndexName() != other.GetIndexName()) return false;

  // If exists
  if (IsIfExists() != other.IsIfExists()) return false;

  return AbstractPlanNode::operator==(rhs);
}
}  // namespace terrier::plan_node
