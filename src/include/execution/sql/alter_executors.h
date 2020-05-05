#pragma once

#include <unordered_map>
#include <vector>

#include "catalog/catalog_accessor.h"
#include "catalog/catalog_defs.h"
#include "catalog/schema.h"
#include "common/managed_pointer.h"
#include "execution/exec_defs.h"
#include "planner/plannodes/alter_plan_node.h"

namespace terrier::execution::sql {

/**
 * static utility class to execute ONE ALTER table command
 */
class AlterTableCmdExecutor {
 public:
  AlterTableCmdExecutor() = delete;

  /**
   * Record the changes to a specific column
   */

  /**
   *
   * @param cmd  AddColumn Command
   * @param accessor accessor to use for this execution
   * @return whether add column is successful
   */
  static bool AddColumn(const common::ManagedPointer<planner::AlterCmdBase> &cmd,
                        const std::unique_ptr<catalog::Schema> &schema,
                        common::ManagedPointer<catalog::CatalogAccessor> accessor, const ChangeMap &change_map);

  /**
   * Drops a column
   * @param cmd DropColumn command
   * @param schema Schema to accumulate changes
   * @param accessor catalog accessor
   * @param change_map  to record the changes to schema
   * @return whether drop column is successful
   */
  static bool DropColumn(const common::ManagedPointer<planner::AlterCmdBase> &cmd,
                         const std::unique_ptr<catalog::Schema> &schema,
                         common::ManagedPointer<catalog::CatalogAccessor> accessor, const ChangeMap &change_map);
};

}  // namespace terrier::execution::sql
