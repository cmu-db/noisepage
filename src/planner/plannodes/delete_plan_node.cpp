#include "planner/plannodes/delete_plan_node.h"
#include <memory>
#include <utility>

namespace terrier::planner {

// TODO(Gus,Wen) Add SetParameters

common::hash_t DeletePlanNode::Hash() const {
  auto type = GetPlanNodeType();
  common::hash_t hash = common::HashUtil::Hash(&type);

  // Hash database_oid
  auto database_oid = GetDatabaseOid();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&database_oid));

  // Hash table_oid
  auto table_oid = GetTableOid();
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&table_oid));

  // Hash delete_condition
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(delete_condition_->Hash()));

  return common::HashUtil::CombineHashes(hash, AbstractPlanNode::Hash());
}

bool DeletePlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (GetPlanNodeType() != rhs.GetPlanNodeType()) return false;

  auto &other = dynamic_cast<const DeletePlanNode &>(rhs);

  // Database OID
  if (GetDatabaseOid() != other.GetDatabaseOid()) return false;

  // Table OID
  if (GetTableOid() != other.GetTableOid()) return false;

  // Delete condition
  if (*GetDeleteCondition() != *other.GetDeleteCondition()) return false;

  return AbstractPlanNode::operator==(rhs);
}
}  // namespace terrier::planner
