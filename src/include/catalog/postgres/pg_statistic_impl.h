#pragma once

#include "catalog/catalog_defs.h"
#include "catalog/postgres/pg_statistic.h"
#include "catalog/schema.h"
#include "common/managed_pointer.h"
#include "storage/projected_row.h"
#include "storage/storage_defs.h"

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

/** The NoisePage version of pg_statistic. */
class PgStatisticImpl {
 private:
  friend class Builder;                   ///< The builder is used to construct pg_statistic.
  friend class storage::RecoveryManager;  ///< The RM accesses tables and indexes without going through the catalog.
  friend class catalog::DatabaseCatalog;  ///< DatabaseCatalog sets up and owns pg_statistic.

  /**
   * @brief Prepare to create pg_statistic.
   *
   * Does NOT create anything until the relevant bootstrap functions are called.
   *
   * @param db_oid          The OID of the database that pg_statistic should be created in.
   */
  explicit PgStatisticImpl(db_oid_t db_oid);

  /** @brief Bootstrap the projected row initializers for pg_statistic. */
  void BootstrapPRIs();

  /**
   * @brief Create pg_statistic and associated indexes.
   *
   * Bootstrap:
   *    pg_statistic
   *    pg_statistic_index
   *
   * Dependencies (for bootstrapping):
   *    pg_core must have been bootstrapped.
   * Dependencies (for execution):
   *    No other dependencies.
   *
   * @param txn             The transaction to bootstrap in.
   * @param dbc             The catalog object to bootstrap in.
   */
  void Bootstrap(common::ManagedPointer<transaction::TransactionContext> txn,
                 common::ManagedPointer<DatabaseCatalog> dbc);

  /**
   * Add a column statistic entry to pg_statistic.
   *
   * @param txn         The transaction to use.
   * @param table_oid   The OID of the table.
   * @param col_oid     The OID of the column.
   * @param col         The column to insert.
   * @return            True if the insert succeeded. False otherwise.
   */
  void CreateColumnStatistic(common::ManagedPointer<transaction::TransactionContext> txn, table_oid_t table_oid,
                             col_oid_t col_oid, const Schema::Column &col);

  /**
   * Delete all column statistic entries for a particular table from pg_statistic.
   *
   * @param txn         The transaction to use.
   * @param class_oid   The OID of the table to delete column statistics for.
   * @return            True if the delete was successful. False otherwise.
   */
  bool DeleteColumnStatistics(common::ManagedPointer<transaction::TransactionContext> txn, table_oid_t table_oid);

  const db_oid_t db_oid_;

  /**
   * The table and indexes that define pg_statistic.
   * Created by: Builder::CreateDatabaseCatalog.
   * Cleaned up by: DatabaseCatalog::TearDown, where the scans from pg_class and pg_index pick these up.
   */
  ///@{
  common::ManagedPointer<storage::SqlTable> statistics_;
  common::ManagedPointer<storage::index::Index> statistic_oid_index_;  // indexed on starelid, staattnum
  storage::ProjectedRowInitializer pg_statistic_all_cols_pri_;
  storage::ProjectionMap pg_statistic_all_cols_prm_;
  storage::ProjectedRowInitializer statistic_oid_index_pri_;
  storage::ProjectionMap statistic_oid_index_prm_;
  ///@}
};

}  // namespace noisepage::catalog::postgres
