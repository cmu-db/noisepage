#pragma once

#include <string>

#include "catalog/catalog_defs.h"
#include "common/managed_pointer.h"
namespace terrier::planner {
class CreateDatabasePlanNode;
class CreateNamespacePlanNode;
class CreateTablePlanNode;
class CreateIndexPlanNode;
class CreateViewPlanNode;
class DropDatabasePlanNode;
class DropNamespacePlanNode;
class DropTablePlanNode;
class DropIndexPlanNode;
}  // namespace terrier::planner

namespace terrier::catalog {
class CatalogAccessor;
class IndexSchema;
}  // namespace terrier::catalog

namespace terrier::execution::sql {

/**
 * static utility class to execute DDL plan nodes, can be called directly by C++ or eventually through TPL builtins
 */
class DDLExecutors {
 public:
  DDLExecutors() = delete;

  /**
   * @param node node to executed
   * @param accessor accessor to use for execution
   * @return true if operation succeeded, false otherwise
   */
  static bool CreateDatabaseExecutor(common::ManagedPointer<planner::CreateDatabasePlanNode> node,
                                     common::ManagedPointer<catalog::CatalogAccessor> accessor);

  /**
   * @param node node to executed
   * @param accessor accessor to use for execution
   * @return true if operation succeeded, false otherwise
   */
  static bool CreateNamespaceExecutor(common::ManagedPointer<planner::CreateNamespacePlanNode> node,
                                      common::ManagedPointer<catalog::CatalogAccessor> accessor);

  /**
   * @param node node to executed
   * @param accessor accessor to use for execution
   * @param connection_db database for the current connection
   * @return true if operation succeeded, false otherwise
   */
  static bool CreateTableExecutor(common::ManagedPointer<planner::CreateTablePlanNode> node,
                                  common::ManagedPointer<catalog::CatalogAccessor> accessor,
                                  catalog::db_oid_t connection_db);

  /**
   * @param node node to executed
   * @param accessor accessor to use for execution
   * @return true if operation succeeded, false otherwise
   */
  static bool CreateIndexExecutor(common::ManagedPointer<planner::CreateIndexPlanNode> node,
                                  common::ManagedPointer<catalog::CatalogAccessor> accessor);

  /**
   * @param node node to executed
   * @param accessor accessor to use for execution
   * @param connection_db database for the current connection
   * @return true if operation succeeded, false otherwise
   */
  static bool DropDatabaseExecutor(common::ManagedPointer<planner::DropDatabasePlanNode> node,
                                   common::ManagedPointer<catalog::CatalogAccessor> accessor,
                                   catalog::db_oid_t connection_db);

  /**
   * @param node node to executed
   * @param accessor accessor to use for execution
   * @return true if operation succeeded, false otherwise
   */
  static bool DropNamespaceExecutor(common::ManagedPointer<planner::DropNamespacePlanNode> node,
                                    common::ManagedPointer<catalog::CatalogAccessor> accessor);

  /**
   * @param node node to executed
   * @param accessor accessor to use for execution
   * @return true if operation succeeded, false otherwise
   */
  static bool DropTableExecutor(common::ManagedPointer<planner::DropTablePlanNode> node,
                                common::ManagedPointer<catalog::CatalogAccessor> accessor);

  /**
   * @param node node to executed
   * @param accessor accessor to use for execution
   * @return true if operation succeeded, false otherwise
   */
  static bool DropIndexExecutor(common::ManagedPointer<planner::DropIndexPlanNode> node,
                                common::ManagedPointer<catalog::CatalogAccessor> accessor);

 private:
  static bool CreateIndex(common::ManagedPointer<catalog::CatalogAccessor> accessor, catalog::namespace_oid_t ns,
                          const std::string &name, catalog::table_oid_t table,
                          const catalog::IndexSchema &input_schema);
};
}  // namespace terrier::execution::sql
