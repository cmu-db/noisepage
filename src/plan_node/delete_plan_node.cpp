#include "plan_node/delete_plan_node.h"
#include "storage/data_table.h"

namespace terrier::plan_node {

// TODO(Gus,Wen) Add SetParameters
// TODO(Gus,Wen) Do catalog lookups once catalog is available

DeletePlanNode::DeletePlanNode(std::shared_ptr<storage::SqlTable> target_table)
    : target_table_(std::move(target_table)) {}

DeletePlanNode::DeletePlanNode(parser::DeleteStatement *delete_stmt) {
  table_name_ = delete_stmt->GetDeletionTable()->GetTableName();
  delete_condition_ = delete_stmt->GetDeleteCondition();

  // TODO(Gus,Wen) Get table from catalog once catalog is available
}

}  // namespace terrier::plan_node
