#include "plan_node/update_plan_node.h"
#include <memory>
#include <utility>
#include "plan_node/abstract_scan_plan_node.h"

namespace terrier::plan_node {

common::hash_t UpdatePlanNode::Hash() const {
  auto type = GetPlanNodeType();
  common::hash_t hash = common::HashUtil::Hash(&type);

  // Hash table_oid
  auto table_oid = GetTableOid();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&table_oid));

  // Hash table_name
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(GetTableName()));

  // Hash update_primary_key
  auto is_update_primary_key = GetUpdatePrimaryKey();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&is_update_primary_key));

  return common::HashUtil::CombineHashes(hash, AbstractPlanNode::Hash());
}

bool UpdatePlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (GetPlanNodeType() != rhs.GetPlanNodeType()) return false;

  // Target table OID
  auto &other = static_cast<const plan_node::UpdatePlanNode &>(rhs);
  if (GetTableOid() != other.GetTableOid()) return false;

  // Update table name
  if (GetTableName() != other.GetTableName()) return false;

  // Update primary key
  if (GetUpdatePrimaryKey() != other.GetUpdatePrimaryKey()) return false;

  return AbstractPlanNode::operator==(rhs);
}

}  // namespace terrier::plan_node
