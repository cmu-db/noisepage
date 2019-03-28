#include "plan_node/delete_plan_node.h"
#include <memory>
#include <utility>

namespace terrier::plan_node {

// TODO(Gus,Wen) Add SetParameters

common::hash_t DeletePlanNode::Hash() const {
  auto type = GetPlanNodeType();
  common::hash_t hash = common::HashUtil::Hash(&type);

  // Hash target_table_oid
  auto target_table_oid = GetTargetTableOid();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&target_table_oid));

  // Hash table_name
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(GetTableName()));

  // Hash delete_condition
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(delete_condition_->Hash()));

  return common::HashUtil::CombineHashes(hash, AbstractPlanNode::Hash());
}

bool DeletePlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (GetPlanNodeType() != rhs.GetPlanNodeType()) return false;

  auto &other = dynamic_cast<const DeletePlanNode &>(rhs);

  // Target table OID
  if (GetTargetTableOid() != other.GetTargetTableOid()) return false;

  // Table name
  if (GetTableName() != other.GetTableName()) return false;

  // Delete condition
  if (*GetDeleteCondition() != *other.GetDeleteCondition()) return false;

  return AbstractPlanNode::operator==(rhs);
}
}  // namespace terrier::plan_node
