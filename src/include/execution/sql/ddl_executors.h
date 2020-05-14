#pragma once

#include <string>

#include "catalog/catalog_defs.h"
#include "catalog/schema.h"
#include "common/managed_pointer.h"
namespace terrier::planner {
class CreateDatabasePlanNode;
class CreateNamespacePlanNode;
class CreateTablePlanNode;
class CreateIndexPlanNode;
class DropDatabasePlanNode;
class DropNamespacePlanNode;
class DropTablePlanNode;
class DropIndexPlanNode;
}  // namespace terrier::planner

namespace terrier::execution::exec {
class Executionaccessor;
}

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

  static catalog::index_oid_t CreateIndexForConstraints(common::ManagedPointer<catalog::CatalogAccessor> accessor,
                                                        catalog::namespace_oid_t ns, const std::string &name,
                                                        catalog::table_oid_t table,
                                                        const catalog::IndexSchema &input_schema);
  /**
   * Loop through all the planNode constraint information, create the respect constraints
   * By calling the access API accordingly
   * @param accessor database catalog accessor
   * @param table the table oid of the table the constraints is creating for
   * @param plan_node the plan_node pointer that contains all the constraint info
   * @return true if all constraints created successfully, false is any constraint was not successful, abort
   */
  static bool CreatePKConstraintsAndIndices(common::ManagedPointer<catalog::CatalogAccessor> accessor,
                                            const catalog::Schema &schema,  catalog::table_oid_t table,
                                            common::ManagedPointer<planner::CreateTablePlanNode> plan_node,
                                            catalog::db_oid_t connection_db);
  static bool CreateFKConstraintsAndIndices( common::ManagedPointer<catalog::CatalogAccessor> accessor,
                                            const catalog::Schema &schema,  catalog::table_oid_t table,
                                             common::ManagedPointer<planner::CreateTablePlanNode> plan_node,
                                             catalog::db_oid_t connection_db);
  static bool CreateUniqueConstraintsAndIndices( common::ManagedPointer<catalog::CatalogAccessor> accessor,
                                                const catalog::Schema &schema,  catalog::table_oid_t table,
                                                 common::ManagedPointer<planner::CreateTablePlanNode> plan_node,
                                                 catalog::db_oid_t connection_db);
  static bool CreateCheckConstraintsAndIndices( common::ManagedPointer<catalog::CatalogAccessor> accessor,
                                               const catalog::Schema &schema,  catalog::table_oid_t table,
                                                common::ManagedPointer<planner::CreateTablePlanNode> plan_node,
                                                catalog::db_oid_t connection_db);
  static bool CreateExclusionConstraintsAndIndices( common::ManagedPointer<catalog::CatalogAccessor> accessor,
                                                   const catalog::Schema &schema,  catalog::table_oid_t table,
                                                    common::ManagedPointer<planner::CreateTablePlanNode> plan_node,
                                                    catalog::db_oid_t connection_db);
  static bool CreateNotNullConstraints( common::ManagedPointer<catalog::CatalogAccessor> accessor,
                                       const catalog::Schema &schema,  catalog::table_oid_t table,
                                        common::ManagedPointer<planner::CreateTablePlanNode> plan_node,
                                        catalog::db_oid_t connection_db);
  static bool CreateTriggerConstraints( common::ManagedPointer<catalog::CatalogAccessor> accessor,
                                       const catalog::Schema &schema,  catalog::table_oid_t table,
                                        common::ManagedPointer<planner::CreateTablePlanNode> plan_node,
                                        catalog::db_oid_t connection_db);
};
}  // namespace terrier::execution::sql
