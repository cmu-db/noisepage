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
   * Record the changes to a specific column
   */
  enum class ChangeType { Add, Drop, ChangeDefault };
  using ChangeMap = std::unordered_map<std::string, std::vector<ChangeType>>;

  /**
   *
   * @param cmd  AddColumn Command
   * @param accessor accessor to use for this execution
   * @return
   */
  static bool AddColumn(const common::ManagedPointer<planner::AlterCmdBase> &cmd,
                        std::unique_ptr<catalog::Schema> &schema,
                        const common::ManagedPointer<catalog::CatalogAccessor> accessor,
                        std::unordered_map<std::string, std::vector<ChangeType>> &change_map);
};

}  // namespace terrier::execution::sql
