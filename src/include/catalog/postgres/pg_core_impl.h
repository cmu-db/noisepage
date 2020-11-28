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

  /** Bootstrap the projected row initializers for pg_attribute. */
  void BootstrapPRIs();

  /**
   * Bootstrap:
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
                                          const common::ManagedPointer<storage::GarbageCollector> garbage_collector);

  bool CreateNamespace(const common::ManagedPointer<transaction::TransactionContext> txn, const std::string &name,
                       const namespace_oid_t ns_oid);
  bool DeleteNamespace(const common::ManagedPointer<transaction::TransactionContext> txn,
                       const common::ManagedPointer<DatabaseCatalog> dbc, const namespace_oid_t ns_oid);
  namespace_oid_t GetNamespaceOid(const common::ManagedPointer<transaction::TransactionContext> txn,
                                  const std::string &name);
  std::vector<std::pair<uint32_t, postgres::ClassKind>> GetNamespaceClassOids(
      const common::ManagedPointer<transaction::TransactionContext> txn, const namespace_oid_t ns_oid);

  template <typename ClassOid, typename Ptr>
  bool SetClassPointer(const common::ManagedPointer<transaction::TransactionContext> txn, const ClassOid oid,
                       const Ptr *const pointer, const col_oid_t class_col);
  bool CreateTableEntry(const common::ManagedPointer<transaction::TransactionContext> txn, const table_oid_t table_oid,
                        const namespace_oid_t ns_oid, const std::string &name, const Schema &schema);
  bool DeleteTable(const common::ManagedPointer<transaction::TransactionContext> txn,
                   const common::ManagedPointer<DatabaseCatalog> dbc, const table_oid_t table);

  bool CreateIndexEntry(const common::ManagedPointer<transaction::TransactionContext> txn, const namespace_oid_t ns_oid,
                        const table_oid_t table_oid, const index_oid_t index_oid, const std::string &name,
                        const IndexSchema &schema);
  bool DeleteIndex(const common::ManagedPointer<transaction::TransactionContext> txn,
                   common::ManagedPointer<DatabaseCatalog> dbc, index_oid_t index);
  std::vector<std::pair<common::ManagedPointer<storage::index::Index>, const IndexSchema &>> GetIndexes(
      const common::ManagedPointer<transaction::TransactionContext> txn, table_oid_t table);
  std::vector<index_oid_t> GetIndexOids(const common::ManagedPointer<transaction::TransactionContext> txn,
                                        table_oid_t table);

  std::pair<void *, postgres::ClassKind> GetClassPtrKind(
      const common::ManagedPointer<transaction::TransactionContext> txn, uint32_t oid);
  std::pair<void *, postgres::ClassKind> GetClassSchemaPtrKind(
      const common::ManagedPointer<transaction::TransactionContext> txn, uint32_t oid);
  std::pair<uint32_t, postgres::ClassKind> GetClassOidKind(
      const common::ManagedPointer<transaction::TransactionContext> txn, const namespace_oid_t ns_oid,
      const std::string &name);

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
   */
  template <typename Column, typename ClassOid>
  bool DeleteColumns(common::ManagedPointer<transaction::TransactionContext> txn, ClassOid class_oid);

 private:
  friend class Builder;
  friend class storage::RecoveryManager;

  const db_oid_t db_oid_;

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

  /**
   * @tparam Column         The type of column (Schema::Column or IndexSchema::Column).
   * @param pr              The ProjectedRow to populate.
   * @param table_pm        The ProjectionMap for the ProjectedRow.
   * @return The requested column.
   */
  template <typename Column, typename ColOid>
  static Column MakeColumn(storage::ProjectedRow *pr, const storage::ProjectionMap &pr_map);

  storage::SqlTable *namespaces_;
  storage::index::Index *namespaces_oid_index_;
  storage::index::Index *namespaces_name_index_;
  storage::ProjectedRowInitializer pg_namespace_all_cols_pri_;
  storage::ProjectionMap pg_namespace_all_cols_prm_;
  storage::ProjectedRowInitializer delete_namespace_pri_;
  storage::ProjectedRowInitializer get_namespace_pri_;

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

  storage::SqlTable *indexes_;
  storage::index::Index *indexes_oid_index_;
  storage::index::Index *indexes_table_index_;
  storage::ProjectedRowInitializer get_indexes_pri_;
  storage::ProjectedRowInitializer delete_index_pri_;
  storage::ProjectionMap delete_index_prm_;
  storage::ProjectedRowInitializer pg_index_all_cols_pri_;
  storage::ProjectionMap pg_index_all_cols_prm_;

  storage::SqlTable *columns_;
  storage::index::Index *columns_oid_index_;   // indexed on class OID and column OID
  storage::index::Index *columns_name_index_;  // indexed on class OID and column name
  storage::ProjectedRowInitializer pg_attribute_all_cols_pri_;
  storage::ProjectionMap pg_attribute_all_cols_prm_;
  storage::ProjectedRowInitializer get_columns_pri_;
  storage::ProjectionMap get_columns_prm_;
  storage::ProjectedRowInitializer delete_columns_pri_;
  storage::ProjectionMap delete_columns_prm_;
};

}  // namespace noisepage::catalog::postgres
