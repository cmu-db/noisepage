#include "plan_node/delete_plan_node.h"
#include "storage/sql_table.h"

namespace terrier::plan_node {

// TODO(Gus,Wen) Add SetParameters
// TODO(Gus,Wen) Do catalog lookups once catalog is available

DeletePlanNode::DeletePlanNode(std::shared_ptr<storage::SqlTable> target_table)
    : AbstractPlanNode(nullptr), target_table_(std::move(target_table)) {}

DeletePlanNode::DeletePlanNode(parser::DeleteStatement *delete_stmt) {
  table_name_ = delete_stmt->GetDeletionTable()->GetTableName();
  delete_condition_ = delete_stmt->GetDeleteCondition();

  // TODO(Gus,Wen) Get table from catalog once catalog is available
}

common::hash_t DeletePlanNode::Hash() const {
  auto type = GetPlanNodeType();
  common::hash_t hash = common::HashUtil::Hash(&type);

  // TODO(Gus,Wen) Add hash for target table

  return common::HashUtil::CombineHashes(hash, AbstractPlanNode::Hash());
}

bool DeletePlanNode::operator==(const AbstractPlanNode &rhs) const {
  if (GetPlanNodeType() != rhs.GetPlanNodeType()) return false;

  // TODO(Gus, Wen) Compare target tables

  return AbstractPlanNode::operator==(rhs);
}

}  // namespace terrier::plan_node
