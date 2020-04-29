#include "execution/sql/alter_executors.h"


#include "catalog/catalog_accessor.h"
#include "storage/sql_table.h"


namespace terrier::execution::sql {
bool AlterTableCmdExecutor::AddColumn(const common::ManagedPointer<planner::AlterCmdBase> &cmd,
                                      std::unique_ptr<catalog::Schema> &schema,
                                      const common::ManagedPointer<catalog::CatalogAccessor> accessor)
{
  auto add_col_cmd = cmd.CastManagedPointerTo<planner::AlterPlanNode::AddColumnCmd>();
  auto new_col = add_col_cmd->GetColumn();
  auto cols = schema->GetColumns();
  cols.push_back(new_col);

  // Update the schema
  auto tmp_schema = std::make_unique<catalog::Schema>(cols);
  schema.swap(tmp_schema);

  return true;
}
} // namespace terrier::execution::sql