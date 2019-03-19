#include "plan_node/delete_plan_node.h"
#include <memory>
#include <utility>
#include "storage/sql_table.h"

namespace terrier::plan_node {

// TODO(Gus,Wen) Add SetParameters

DeletePlanNode::DeletePlanNode(catalog::table_oid_t target_table_oid)
    : AbstractPlanNode(nullptr), target_table_oid_(target_table_oid) {}

DeletePlanNode::DeletePlanNode(parser::DeleteStatement *delete_stmt) {
  table_name_ = delete_stmt->GetDeletionTable()->GetTableName();
  delete_condition_ = delete_stmt->GetDeleteCondition();

  // TODO(Gus,Wen) Get table OID from catalog once catalog is available
}

common::hash_t DeletePlanNode::Hash() const {
  auto type = GetPlanNodeType();
  common::hash_t hash = common::HashUtil::Hash(&type);

  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(&target_table_oid_));

  return common::HashUtil::CombineHashes(hash, AbstractPlanNode::Hash());
}

bool DeletePlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (GetPlanNodeType() != rhs.GetPlanNodeType()) return false;

  auto &other = static_cast<const plan_node::DeletePlanNode &>(rhs);

  if (GetTargetTableOid() != other.GetTargetTableOid()) return false;

  return AbstractPlanNode::operator==(rhs);
}

}  // namespace terrier::plan_node
