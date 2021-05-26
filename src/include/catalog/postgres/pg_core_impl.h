#pragma once

#include <string>
#include <utility>
#include <vector>

#include "catalog/catalog_defs.h"
#include "catalog/postgres/pg_class.h"
#include "common/managed_pointer.h"
#include "storage/projected_row.h"
#include "storage/storage_defs.h"

namespace noisepage::catalog {
class DatabaseCatalog;
class IndexSchema;
class Schema;
}  // namespace noisepage::catalog

namespace noisepage::execution::functions {
class FunctionContext;
}  // namespace noisepage::execution::functions

namespace noisepage::storage {
class GarbageCollector;
class RecoveryManager;
class SqlTable;

namespace index {
class Index;
}  // namespace index
}  // namespace noisepage::storage

namespace noisepage::transaction {
class TransactionContext;
}  // namespace noisepage::transaction

namespace noisepage::catalog::postgres {
class Builder;

/** The NoisePage version of pg_namespace, pg_class, pg_index, and pg_attribute. */
class PgCoreImpl {
 private:
  friend class Builder;                   ///< The builder is used to construct the core catalog tables.
  friend class catalog::DatabaseCatalog;  ///< DatabaseCatalog sets up and owns the core catalog tables.
  friend class storage::RecoveryManager;  ///< The RM accesses tables and indexes without going through the catalog.

  /**
   * @brief Prepare to create the core catalog tables: pg_namespace, pg_class, pg_index, and pg_attribute.
   *
   * Does NOT create anything until the relevant bootstrap functions are called.
   *
   * @param db_oid  The OID of the database that the core catalog tables should be created in.
   */
  explicit PgCoreImpl(db_oid_t db_oid);

  /** @brief Bootstrap the projected row initializers for the core catalog tables. */
  void BootstrapPRIs();

  /**
   * @brief Create all the catalog namespaces, tables, and indexes for core catalog functionality.
   *
   * Bootstrap:
   *   The "pg_catalog" and "public" namespaces.
   *   pg_namespace
   *   pg_namespace_oid_index
   *   pg_namespace_name_index
   *   pg_class
   *   pg_class_oid_index
   *   pg_class_name_index
   *   pg_class_namespace_index
   *   pg_index
   *   pg_index_oid_index
   *   pg_index_table_index
   *   pg_attribute
   *   pg_attribute_oid_index
   *   pg_attribute_name_index
   *
   * Dependencies:
   *   None
   *
   * @param txn     The transaction to bootstrap in.
   * @param dbc     The database catalog that this PgCoreImpl instance resides in.
   */
  void Bootstrap(common::ManagedPointer<transaction::TransactionContext> txn,
                 common::ManagedPointer<DatabaseCatalog> dbc);

  /**
   * @brief Obtain a function that will teardown the core catalog objects as they exist at the time of the provided txn.
   *
   * @param txn     The transaction to perform the teardown in.
   * @param dbc     The database catalog that this PgCoreImpl instance resides in.
   * @return        A function that will teardown the core catalog objects when invoked.
   */
  std::function<void(void)> GetTearDownFn(common::ManagedPointer<transaction::TransactionContext> txn,
                                          common::ManagedPointer<DatabaseCatalog> dbc);

  /**
   * @brief Create a namespace with the given namespace oid.
   *
   * @param txn     The transaction to use for the operation.
   * @param name    The name of the namespace.
   * @param ns_oid  The oid of the created namespace.
   * @return        True if creation is successful. False otherwise.
   */
  bool CreateNamespace(common::ManagedPointer<transaction::TransactionContext> txn, const std::string &name,
                       namespace_oid_t ns_oid);
  /**
   * @brief Delete the namespace and any objects assigned to the namespace.
   *
   * The 'public' namespace cannot be deleted.
   * This will fail if any objects within the namespace cannot be deleted, i.e., write-write conflicts exist.
   *
   * @param txn     The transaction to use for the operation.
   * @param dbc     The database catalog that this PgCoreImpl instance resides in.
   * @param ns_oid  The oid of the namespace to delete.
   * @return        True if the deletion is successful. False otherwise.
   */
  bool DeleteNamespace(common::ManagedPointer<transaction::TransactionContext> txn,
                       common::ManagedPointer<DatabaseCatalog> dbc, namespace_oid_t ns_oid);
  /**
   * @brief Get the OID of the specified namespace.
   *
   * @param txn     The transaction to use for the operation.
   * @param ns_oid  The OID of the namespace to query.
   * @return        The OID of the specified namespace.
   */
  namespace_oid_t GetNamespaceOid(common::ManagedPointer<transaction::TransactionContext> txn, const std::string &name);
  /**
   * @brief Get a list of all OIDs and their PgClass::RelKind from pg_class for all objects on the given namespace.
   *
   * @param txn     The transaction to use for the operation.
   * @param ns_oid  The OID of the namespace to query.
   * @return        A list of all OIDs and their PgClass::RelKind for all objects on the given namespace.
   */
  std::vector<std::pair<uint32_t, PgClass::RelKind>> GetNamespaceClassOids(
      common::ManagedPointer<transaction::TransactionContext> txn, namespace_oid_t ns_oid);

  /**
   * @brief Insert the provided pointer into the specified pg_class column.
   *
   * @tparam ClassOid   (table_oid_t) OR (index_oid_t)
   * @tparam Ptr        (SqlTable or Schema) OR (Index or IndexSchema), depending on ClassOid
   * @param txn         The transaction to insert in.
   * @param oid         The OID of the object.
   * @param pointer     The pointer to be inserted.
   * @param class_col   The pg_class column to insert the pointer into.
   * @return            True if successful. False otherwise.
   */
  template <typename ClassOid, typename Ptr>
  bool SetClassPointer(common::ManagedPointer<transaction::TransactionContext> txn, ClassOid oid, const Ptr *pointer,
                       col_oid_t class_col);
  /**
   * @brief Create a table.
   *
   * @param txn         The transaction to create a table in.
   * @param table_oid   The OID of the table to be created.
   * @param ns_oid      The OID of the namespace to create a table in.
   * @param name        The name of the table to be created.
   * @param schema      The schema to use for the created table.
   * @return            True if successful. False otherwise.
   */
  bool CreateTableEntry(common::ManagedPointer<transaction::TransactionContext> txn, table_oid_t table_oid,
                        namespace_oid_t ns_oid, const std::string &name, const Schema &schema);
  /**
   * @brief Delete a table and all the table's child objects (e.g., columns, indexes) from the database.
   *
   * @param txn         The transaction to delete in.
   * @param dbc         The database catalog that this PgCoreImpl instance resides in.
   * @param table       The OID of the table to delete.
   * @return            True if successful. False otherwise.
   */
  bool DeleteTable(common::ManagedPointer<transaction::TransactionContext> txn,
                   common::ManagedPointer<DatabaseCatalog> dbc, table_oid_t table);

  /**
   * @brief Rename a table.
   *
   * @param txn         The transaction to rename the table in.
   * @param dbc         The database catalog that this PgCoreImpl instance resides in.
   * @param table       The table to be renamed.
   * @param name        The new name for the table.
   * @return            True if the rename succeeded. False otherwise.
   */
  bool RenameTable(common::ManagedPointer<transaction::TransactionContext> txn,
                   common::ManagedPointer<DatabaseCatalog> dbc, table_oid_t table, const std::string &name);

  /**
   * @brief Create an index.
   *
   * @param txn         The transaction to create an index in.
   * @param ns_oid      The OID of the namespace to create an index in.
   * @param table_oid   The OID of the table to create an index on.
   * @param index_oid   The OID of the index to be created.
   * @param name        The name of the index to be created.
   * @param schema      The index schema to use for the created index.
   * @return            True if successful. False otherwise.
   */
  bool CreateIndexEntry(common::ManagedPointer<transaction::TransactionContext> txn, namespace_oid_t ns_oid,
                        table_oid_t table_oid, index_oid_t index_oid, const std::string &name,
                        const IndexSchema &schema);
  /**
   * @brief Delete an index.
   *
   * @warning       Any constraints that utilize this index must be deleted
   *                or transitioned to a different index prior to deleting an index.
   *
   * @param txn     The transaction to delete the index in.
   * @param index   The OID of the index to be deleted.
   * @return        True if the deletion succeeded. False otherwise.
   */
  bool DeleteIndex(common::ManagedPointer<transaction::TransactionContext> txn,
                   common::ManagedPointer<DatabaseCatalog> dbc, index_oid_t index);
  /**
   * @brief Get index pointers and schemas for every index on a table.
   *
   * Provides much better performance than individual calls to GetIndex and GetIndexSchema.
   *
   * @param txn     The transaction to query in.
   * @param table   The OID of the table to be queried.
   * @return        A vector of pairs of index pointers and their corresponding schemas.
   */
  std::vector<std::pair<common::ManagedPointer<storage::index::Index>, const IndexSchema &>> GetIndexes(
      common::ManagedPointer<transaction::TransactionContext> txn, table_oid_t table);
  /**
   * @brief Get a list of all the indexes for a particular table, given as OIDs.
   *
   * @param txn     The transaction used for the operation.
   * @param table   The table whose indexes are being requested.
   * @return        The indexes for the identified table at the time of the transaction.
   */
  std::vector<index_oid_t> GetIndexOids(common::ManagedPointer<transaction::TransactionContext> txn, table_oid_t table);

  /**
   * @brief Get an object pointer from pg_class.
   *
   * @param txn     The transaction to query in.
   * @param oid     The OID of the object.
   * @return        A pair of a pointer to, and the RelKind, of the object requested.
   *                The pointer will be nullptr if no entry was found for the given oid.
   */
  std::pair<void *, PgClass::RelKind> GetClassPtrKind(common::ManagedPointer<transaction::TransactionContext> txn,
                                                      uint32_t oid);
  /**
   * @brief Get a schema pointer from pg_class.
   *
   * @param txn     The transaction to query in.
   * @param oid     The OID of the object.
   * @return        A pair of a pointer to the schema, and the RelKind, of the object requested.
   *                The pointer will be nullptr if no entry was found for the given oid.
   */
  std::pair<void *, PgClass::RelKind> GetClassSchemaPtrKind(common::ManagedPointer<transaction::TransactionContext> txn,
                                                            uint32_t oid);
  /**
   * @brief Get the OID and kind from pg_class.
   *
   * @param txn     The transaction to query in.
   * @param ns_oid  The namespace the object lives in.
   * @param oid     The name of the object (e.g., table, index, view).
   * @return        A pair of the OID and the RelKind of the object requested.
   */
  std::pair<uint32_t, PgClass::RelKind> GetClassOidKind(common::ManagedPointer<transaction::TransactionContext> txn,
                                                        namespace_oid_t ns_oid, const std::string &name);

  /**
   * Add a new column entry in pg_attribute.
   *
   * @tparam Column     The type of column (Schema::Column or IndexSchema::Column).
   * @param txn         The transaction to use.
   * @param class_oid   The OID of the table (or index).
   * @param col_oid     The OID of the column to insert.
   * @param col         The column to insert.
   * @return            True if the insert succeeded. False otherwise.
   */
  template <typename Column, typename ClassOid, typename ColOid>
  bool CreateColumn(common::ManagedPointer<transaction::TransactionContext> txn, ClassOid class_oid, ColOid col_oid,
                    const Column &col);
  /**
   * Get the columns corresponding to a particular entry in pg_attribute.
   *
   * @tparam Column         The type of column (Schema::Column or IndexSchema::Column).
   * @param txn             The transaction to use.
   * @param class_oid       The OID of the table (or index).
   * @return                The columns corresponding to the entry.
   */
  template <typename Column, typename ClassOid, typename ColOid>
  std::vector<Column> GetColumns(common::ManagedPointer<transaction::TransactionContext> txn, ClassOid class_oid);

  /**
   * Delete all columns corresponding to a particular entry in pg_attribute.
   *
   * @tparam Column         The type of column (Schema::Column or IndexSchema::Column).
   * @param txn             The transaction to use.
   * @param class_oid       The OID of the table (or index).
   * @return                True if the delete was successful. False otherwise.
   *
   * TODO(Matt): We need a DeleteColumn.
   */
  template <typename Column, typename ClassOid>
  bool DeleteColumns(common::ManagedPointer<transaction::TransactionContext> txn, ClassOid class_oid);

  /** Bootstrap functions. */
  ///@{
  void BootstrapPRIsPgNamespace();
  void BootstrapPRIsPgClass();
  void BootstrapPRIsPgIndex();
  void BootstrapPRIsPgAttribute();
  void BootstrapPgNamespace(common::ManagedPointer<transaction::TransactionContext> txn,
                            common::ManagedPointer<DatabaseCatalog> dbc);
  void BootstrapPgClass(common::ManagedPointer<transaction::TransactionContext> txn,
                        common::ManagedPointer<DatabaseCatalog> dbc);
  void BootstrapPgIndex(common::ManagedPointer<transaction::TransactionContext> txn,
                        common::ManagedPointer<DatabaseCatalog> dbc);
  void BootstrapPgAttribute(common::ManagedPointer<transaction::TransactionContext> txn,
                            common::ManagedPointer<DatabaseCatalog> dbc);
  ///@}

  /**
   * @tparam Column         The type of column (Schema::Column or IndexSchema::Column).
   * @param pr              The ProjectedRow to populate.
   * @param table_pm        The ProjectionMap for the ProjectedRow.
   * @return                The requested column.
   */
  template <typename Column, typename ColOid>
  static Column MakeColumn(storage::ProjectedRow *pr, const storage::ProjectionMap &pr_map);

  const db_oid_t db_oid_;

  /**
   * The table and indexes that define pg_namespace.
   * Created by: Builder::CreateDatabaseCatalog.
   * Cleaned up by: DatabaseCatalog::TearDown, where the scans from pg_class and pg_index pick these up.
   */
  ///@{
  common::ManagedPointer<storage::SqlTable> namespaces_;
  common::ManagedPointer<storage::index::Index> namespaces_oid_index_;
  common::ManagedPointer<storage::index::Index> namespaces_name_index_;
  storage::ProjectedRowInitializer pg_namespace_all_cols_pri_;
  storage::ProjectionMap pg_namespace_all_cols_prm_;
  storage::ProjectedRowInitializer delete_namespace_pri_;
  storage::ProjectedRowInitializer get_namespace_pri_;
  ///@}

  /**
   * The table and indexes that define pg_class.
   * Created by: Builder::CreateDatabaseCatalog.
   * Cleaned up by: DatabaseCatalog::TearDown, where the scans from pg_class and pg_index pick these up.
   */
  ///@{
  common::ManagedPointer<storage::SqlTable> classes_;
  common::ManagedPointer<storage::index::Index> classes_oid_index_;
  common::ManagedPointer<storage::index::Index> classes_name_index_;  // indexed on namespace OID and name
  common::ManagedPointer<storage::index::Index> classes_namespace_index_;
  storage::ProjectedRowInitializer pg_class_all_cols_pri_;
  storage::ProjectionMap pg_class_all_cols_prm_;
  storage::ProjectedRowInitializer get_class_oid_kind_pri_;
  storage::ProjectedRowInitializer set_class_pointer_pri_;
  storage::ProjectedRowInitializer set_class_schema_pri_;
  storage::ProjectedRowInitializer get_class_pointer_kind_pri_;
  storage::ProjectedRowInitializer get_class_schema_pointer_kind_pri_;
  storage::ProjectedRowInitializer get_class_object_and_schema_pri_;
  storage::ProjectionMap get_class_object_and_schema_prm_;
  ///@}

  /**
   * The table and indexes that define pg_index.
   * Created by: Builder::CreateDatabaseCatalog.
   * Cleaned up by: DatabaseCatalog::TearDown, where the scans from pg_class and pg_index pick these up.
   */
  ///@{
  common::ManagedPointer<storage::SqlTable> indexes_;
  common::ManagedPointer<storage::index::Index> indexes_oid_index_;
  common::ManagedPointer<storage::index::Index> indexes_table_index_;
  storage::ProjectedRowInitializer get_indexes_pri_;
  storage::ProjectedRowInitializer delete_index_pri_;
  storage::ProjectionMap delete_index_prm_;
  storage::ProjectedRowInitializer pg_index_all_cols_pri_;
  storage::ProjectionMap pg_index_all_cols_prm_;
  ///@}

  /**
   * The table and indexes that define pg_attribute.
   * Created by: Builder::CreateDatabaseCatalog.
   * Cleaned up by: DatabaseCatalog::TearDown, where the scans from pg_class and pg_index pick these up.
   */
  ///@{
  common::ManagedPointer<storage::SqlTable> columns_;
  common::ManagedPointer<storage::index::Index> columns_oid_index_;   // indexed on class OID and column OID
  common::ManagedPointer<storage::index::Index> columns_name_index_;  // indexed on class OID and column name
  storage::ProjectedRowInitializer pg_attribute_all_cols_pri_;
  storage::ProjectionMap pg_attribute_all_cols_prm_;
  storage::ProjectedRowInitializer get_columns_pri_;
  storage::ProjectionMap get_columns_prm_;
  storage::ProjectedRowInitializer delete_columns_pri_;
  storage::ProjectionMap delete_columns_prm_;
  ///@}
};

}  // namespace noisepage::catalog::postgres
