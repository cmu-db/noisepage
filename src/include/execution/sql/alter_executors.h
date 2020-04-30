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
   * @return
   */
  static bool AddColumn(const common::ManagedPointer<planner::AlterCmdBase> &cmd,
                        std::unique_ptr<catalog::Schema> &schema,
                        const common::ManagedPointer<catalog::CatalogAccessor> accessor, ChangeMap &change_map);
};

}  // namespace terrier::execution::sql
