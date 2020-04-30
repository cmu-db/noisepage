#include "execution/sql/alter_executors.h"

#include "catalog/catalog_accessor.h"
#include "storage/sql_table.h"

namespace terrier::execution::sql {
bool AlterTableCmdExecutor::AddColumn(const common::ManagedPointer<planner::AlterCmdBase> &cmd,
                                      std::unique_ptr<catalog::Schema> &schema,
                                      const common::ManagedPointer<catalog::CatalogAccessor> accessor,
                                      std::unordered_map<std::string, std::vector<ChangeType>> &change_map) {
  // Add the column
  auto add_col_cmd = cmd.CastManagedPointerTo<planner::AlterPlanNode::AddColumnCmd>();
  auto new_col = add_col_cmd->GetColumn();
  auto cols = schema->GetColumns();
  cols.push_back(new_col);

  // Record the change
  change_map[new_col.Name()].push_back(ChangeType::Add);

  // Generate the new schema
  std::unique_ptr<catalog::Schema> tmp_schema(new catalog::Schema(cols));
  schema.swap(tmp_schema);
  // TODO(SC): adding constrain ?

  return true;
}
}  // namespace terrier::execution::sql