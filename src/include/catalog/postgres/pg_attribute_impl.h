#pragma once

#include <vector>

#include "common/managed_pointer.h"
#include "storage/projected_row.h"
#include "storage/storage_defs.h"

namespace noisepage::catalog {
class DatabaseCatalog;
}  // namespace noisepage::catalog

namespace noisepage::execution::functions {
class FunctionContext;
}  // namespace noisepage::execution::functions

namespace noisepage::storage {
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

/** The NoisePage version of pg_attribute. */
class PgAttributeImpl {
 public:
  /**
   * Prepare to create pg_attribute.
   * Does NOT create anything until the relevant bootstrap functions are called.
   *
   * @param db_oid          The OID of the database that pg_attribute should be created in.
   */
  explicit PgAttributeImpl(db_oid_t db_oid);

  /** Bootstrap the projected row initializers for pg_attribute. */
  void BootstrapPRIs();

  /**
   * Bootstrap:
   *    pg_attribute
   *    pg_attribute_oid_index
   *    pg_attribute_name_index
   *
   * @param dbc             The catalog object to bootstrap in.
   * @param txn             The transaction to bootstrap in.
   */
  void Bootstrap(common::ManagedPointer<DatabaseCatalog> dbc,
                 common::ManagedPointer<transaction::TransactionContext> txn);

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

  /**
   * @tparam Column         The type of column (Schema::Column or IndexSchema::Column).
   * @param pr              The ProjectedRow to populate.
   * @param table_pm        The ProjectionMap for the ProjectedRow.
   * @return The requested column.
   */
  template <typename Column, typename ColOid>
  static Column MakeColumn(storage::ProjectedRow *pr, const storage::ProjectionMap &pr_map);

  const db_oid_t db_oid_;

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
