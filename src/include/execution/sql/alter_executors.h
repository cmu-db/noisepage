#pragma once

#include "catalog/catalog_defs.h"
#include "common/managed_pointer.h"
#include "planner/plannodes/alter_plan_node.h"

namespace terrier::planner {
class AlterPlanNode;
}  // namespace terrier::planner

namespace terrier::catalog {
class CatlaogAccessor;
}  // namespace terrier::catalog

namespace terrier::execution::sql {
/**
 * static utility class to execute ONE ALTER table command
 */
class AlterTableCmdExecutor {
 public:
  AlterTableCmdExecutor() = delete;

  /**
   *
   * @param cmd  AddColumn Command
   * @param accessor accessor to use for this execution
   * @return
   */
  static bool AddColumn(const common::ManagedPointer<planner::AlterCmdBase> &cmd,
                        std::unique_ptr<catalog::Schema> &schema,
                        const common::ManagedPointer<catalog::CatalogAccessor> accessor);
};

}  // namespace terrier::execution::sql
