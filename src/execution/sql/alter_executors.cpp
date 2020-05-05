#include "execution/sql/alter_executors.h"

#include "catalog/catalog_accessor.h"
#include "storage/sql_table.h"

namespace terrier::execution::sql {
bool AlterTableCmdExecutor::AddColumn(const common::ManagedPointer<planner::AlterCmdBase> &cmd,
                                      const std::unique_ptr<catalog::Schema> &schema,
                                      common::ManagedPointer<catalog::CatalogAccessor> accessor,
                                      const ChangeMap &change_map) {
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
  // TODO(SC): adding constraints ?

  return true;
}

bool AlterTableCmdExecutor::DropColumn(const common::ManagedPointer<planner::AlterCmdBase> &cmd,
                                       const std::unique_ptr<catalog::Schema> &schema,
                                       common::ManagedPointer<catalog::CatalogAccessor> accessor,
                                       const ChangeMap &change_map) {
  auto drop_col_cmd = cmd.CastManagedPointerTo<planner::AlterPlanNode::DropColumnCmd>();
  auto drop_col_oid = drop_col_cmd->GetColOid();
  if (drop_col_oid == catalog::INVALID_COLUMN_OID) {
    return drop_col_cmd->IsIfExist();
  }

  auto cols = schema->GetColumns();
  for (auto itr = cols.begin(); itr != cols.end(); ++itr) {
    if (itr->Oid() == drop_col_oid) {
      cols.erase(itr);
      break;
    }
  }
  // Generate new schema
  std::unique_ptr<catalog::Schema> tmp_schema(new catalog::Schema(cols));
  schema.swap(tmp_schema);

  // Record the change
  change_map[drop_col_cmd->GetName()].push_back(ChangeType::DropNoCascade);

  // TODO(Schema-Change): where to handle cascade dropping?
  return true;
}
}  // namespace terrier::execution::sql