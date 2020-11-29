#pragma once

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
 public:
  /**
   * Prepare to create the core catalog tables: pg_namespace, pg_class, pg_index, and pg_attribute.
   * Does NOT create anything until the relevant bootstrap functions are called.
   *
   * @param db_oid          The OID of the database that the core catalog tables should be created in.
   */
  explicit PgCoreImpl(db_oid_t db_oid);

  /** Bootstrap the projected row initializers for the core catalog tables. */
  void BootstrapPRIs();

  /**
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
   */
  void Bootstrap(common::ManagedPointer<transaction::TransactionContext> txn,
                 common::ManagedPointer<DatabaseCatalog> dbc);

  /**
   * Obtain a function that will teardown the core catalog tables as they exist at the time of the provided txn.
   *
   * @param txn             The transaction to perform the teardown in.
   * @return A function that will teardown the core catalog tables when invoked.
   */
  std::function<void(void)> GetTearDownFn(common::ManagedPointer<transaction::TransactionContext> txn,
                                          common::ManagedPointer<storage::GarbageCollector> garbage_collector);

  /**
   * Create a namespace with a given ns oid
   * @param txn transaction to use
   * @param name name of the namespace
   * @param ns_oid oid of the namespace
   * @return true if creation is successful
   */
  bool CreateNamespace(common::ManagedPointer<transaction::TransactionContext> txn, const std::string &name,
                       namespace_oid_t ns_oid);
  /**
   * Deletes the namespace and any objects assigned to the namespace.  The
   * 'public' namespace cannot be deleted.  This operation will fail if any
   * objects within the namespace cannot be deleted (i.e. write-write conflicts
   * exist).
   * @param txn for the operation
   * @param ns_oid OID to be deleted
   * @return true if the deletion succeeded, otherwise false
   */
  bool DeleteNamespace(common::ManagedPointer<transaction::TransactionContext> txn,
                       common::ManagedPointer<DatabaseCatalog> dbc, namespace_oid_t ns_oid);
  namespace_oid_t GetNamespaceOid(common::ManagedPointer<transaction::TransactionContext> txn, const std::string &name);
  /**
   * A list of all oids and their PgClass::ClassKind from pg_class on the given namespace. This is currently
   * designed as an internal function, though could be exposed via the CatalogAccessor if desired in the future.
   * @param txn for the operation
   * @param ns being queried
   * @return vector of OIDs for all of the objects on this namespace
   */
  std::vector<std::pair<uint32_t, PgClass::RelKind>> GetNamespaceClassOids(
      common::ManagedPointer<transaction::TransactionContext> txn, namespace_oid_t ns_oid);

  /**
   * Inserts a provided pointer into a given pg_class column. Can be used for class object and schema pointers
   * Helper method since SetIndexPointer/SetTablePointer and SetIndexSchemaPointer/SetTableSchemaPointer
   * are basically indentical outside of input types
   * @tparam ClassOid either index_oid_t or table_oid_t
   * @tparam Ptr either Index or SqlTable
   * @param txn transaction to query
   * @param oid oid to object
   * @param pointer pointer to set
   * @param class_col pg_class column to insert pointer into
   * @return true if successful
   */
  template <typename ClassOid, typename Ptr>
  bool SetClassPointer(common::ManagedPointer<transaction::TransactionContext> txn, ClassOid oid,
                       const Ptr *const pointer, col_oid_t class_col);
  /**
   *
   * @param txn
   * @param table_oid
   * @param ns_oid
   * @param name
   * @param schema
   * @return
   */
  bool CreateTableEntry(common::ManagedPointer<transaction::TransactionContext> txn, table_oid_t table_oid,
                        namespace_oid_t ns_oid, const std::string &name, const Schema &schema);
  /**
   * Deletes a table and all child objects (i.e columns, indexes, etc.) from
   * the database.
   * @param txn for the operation
   * @param table to be deleted
   * @return true if the deletion succeeded, otherwise false
   */
  bool DeleteTable(common::ManagedPointer<transaction::TransactionContext> txn,
                   common::ManagedPointer<DatabaseCatalog> dbc, table_oid_t table);

  /**
   * Create the catalog entries for a new index.
   * @param txn for the operation
   * @param ns OID of the namespace under which the index will fall
   * @param name of the new index
   * @param table on which the new index exists
   * @param schema describing the new index
   * @return OID of the new index or INVALID_INDEX_OID if creation failed
   */
  bool CreateIndexEntry(common::ManagedPointer<transaction::TransactionContext> txn, namespace_oid_t ns_oid,
                        table_oid_t table_oid, index_oid_t index_oid, const std::string &name,
                        const IndexSchema &schema);
  /**
   * Delete an index.  Any constraints that utilize this index must be deleted
   * or transitioned to a different index prior to deleting an index.
   * @param txn for the operation
   * @param index to be deleted
   * @return true if the deletion succeeded, otherwise false.
   */
  bool DeleteIndex(common::ManagedPointer<transaction::TransactionContext> txn,
                   common::ManagedPointer<DatabaseCatalog> dbc, index_oid_t index);
  /**
   * Returns index pointers and schemas for every index on a table. Provides much better performance than individual
   * calls to GetIndex and GetIndexSchema
   * @param txn transaction to use
   * @param table table to get index objects for
   * @return vector of pairs of index pointers and their corresponding schemas
   */
  std::vector<std::pair<common::ManagedPointer<storage::index::Index>, const IndexSchema &>> GetIndexes(
      common::ManagedPointer<transaction::TransactionContext> txn, table_oid_t table);

  /**
   * Get a list of all the indexes for a particular table, given as OIDs.
   *
   * @param txn     The transaction used for the operation.
   * @param table   The table whose indexes are being requested.
   * @return The indexes for the identified table at the time of the transaction.
   */
  std::vector<index_oid_t> GetIndexOids(common::ManagedPointer<transaction::TransactionContext> txn, table_oid_t table);

  /**
   * Helper function to query an object pointer form pg_class
   * @param txn transaction to query
   * @param oid oid to object
   * @return pair of ptr to and ClassKind of object requested. ptr will be nullptr if no entry was found for the given
   * oid
   */

  std::pair<void *, PgClass::RelKind> GetClassPtrKind(common::ManagedPointer<transaction::TransactionContext> txn,
                                                      uint32_t oid);
  /**
   * Helper function to query a schema pointer form pg_class
   * @param txn transaction to query
   * @param oid oid to object
   * @return pair of ptr to schema and ClassKind of object requested. ptr will be nullptr if no entry was found for the
   * given oid
   */

  std::pair<void *, PgClass::RelKind> GetClassSchemaPtrKind(common::ManagedPointer<transaction::TransactionContext> txn,
                                                            uint32_t oid);

  /**
   * Helper function to query the oid and kind from
   * [pg_class](https://www.postgresql.org/docs/9.3/catalog-pg-class.html)
   * @param txn transaction to query
   * @param namespace_oid the namespace oid
   * @param name name of the table, index, view, etc.
   * @return a pair of oid and ClassKind
   */
  std::pair<uint32_t, PgClass::RelKind> GetClassOidKind(common::ManagedPointer<transaction::TransactionContext> txn,
                                                        namespace_oid_t ns_oid, const std::string &name);

  /**
   * Add a new column entry in pg_attribute.
   *
   * @tparam Column         The type of column (Schema::Column or IndexSchema::Column).
   * @param txn             The transaction to use.
   * @param class_oid       The OID of the table (or index).
   * @param col_oid         The OID of the column to insert.
   * @param col             The column to insert.
   * @return True if the insert succeeded. False otherwise.
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
   * @return The columns corresponding to the entry.
   */
  template <typename Column, typename ClassOid, typename ColOid>
  std::vector<Column> GetColumns(common::ManagedPointer<transaction::TransactionContext> txn, ClassOid class_oid);

  /**
   * Delete all columns corresponding to a particular entry in pg_attribute.
   *
   * @tparam Column         The type of column (Schema::Column or IndexSchema::Column).
   * @param txn             The transaction to use.
   * @param class_oid       The OID of the table (or index).
   * @return True if the delete was successful. False otherwise.
   *
   * TODO(Matt): We need a DeleteColumn.
   */
  template <typename Column, typename ClassOid>
  bool DeleteColumns(common::ManagedPointer<transaction::TransactionContext> txn, ClassOid class_oid);

 private:
  friend class Builder;
  friend class storage::RecoveryManager;

  const db_oid_t db_oid_;

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
   * @return The requested column.
   */
  template <typename Column, typename ColOid>
  static Column MakeColumn(storage::ProjectedRow *pr, const storage::ProjectionMap &pr_map);

  /**
   * The table and indexes that define pg_namespace.
   * Created by: Builder::CreateDatabaseCatalog.
   * Cleaned up by: DatabaseCatalog::TearDown, where the scans from pg_class and pg_index pick these up.
   */
  ///@{
  storage::SqlTable *namespaces_;
  storage::index::Index *namespaces_oid_index_;
  storage::index::Index *namespaces_name_index_;
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
  storage::SqlTable *classes_;
  storage::index::Index *classes_oid_index_;
  storage::index::Index *classes_name_index_;  // indexed on namespace OID and name
  storage::index::Index *classes_namespace_index_;
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
  storage::SqlTable *indexes_;
  storage::index::Index *indexes_oid_index_;
  storage::index::Index *indexes_table_index_;
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
  storage::SqlTable *columns_;
  storage::index::Index *columns_oid_index_;   // indexed on class OID and column OID
  storage::index::Index *columns_name_index_;  // indexed on class OID and column name
  storage::ProjectedRowInitializer pg_attribute_all_cols_pri_;
  storage::ProjectionMap pg_attribute_all_cols_prm_;
  storage::ProjectedRowInitializer get_columns_pri_;
  storage::ProjectionMap get_columns_prm_;
  storage::ProjectedRowInitializer delete_columns_pri_;
  storage::ProjectionMap delete_columns_prm_;
  ///@}
};

}  // namespace noisepage::catalog::postgres
