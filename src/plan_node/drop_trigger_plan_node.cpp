#include "plan_node/drop_trigger_plan_node.h"
#include <string>
#include <utility>

namespace terrier::plan_node {
common::hash_t DropTriggerPlanNode::Hash() const {
  auto type = GetPlanNodeType();
  common::hash_t hash = common::HashUtil::Hash(&type);

  // Hash databse_oid
  auto database_oid = GetDatabaseOid();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&database_oid));

  // Hash trigger_oid
  auto trigger_oid = GetTriggerOid();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&trigger_oid));

  // Hash if_exists_
  auto if_exist = IsIfExists();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&if_exist));

  return common::HashUtil::CombineHashes(hash, AbstractPlanNode::Hash());
}

bool DropTriggerPlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (GetPlanNodeType() != rhs.GetPlanNodeType()) return false;

  auto &other = dynamic_cast<const DropTriggerPlanNode &>(rhs);

  // Database OID
  if (GetDatabaseOid() != other.GetDatabaseOid()) return false;

  // Trigger OID
  if (GetTriggerOid() != other.GetTriggerOid()) return false;

  // If exists
  if (IsIfExists() != other.IsIfExists()) return false;

  return AbstractPlanNode::operator==(rhs);
}
}  // namespace terrier::plan_node
