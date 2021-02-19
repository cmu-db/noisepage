#pragma once

#include <memory>

#include "catalog/catalog_defs.h"
#include "catalog/postgres/pg_statistic.h"
#include "catalog/schema.h"
#include "common/managed_pointer.h"
#include "optimizer/statistics/column_stats.h"
#include "optimizer/statistics/table_stats.h"
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

namespace noisepage::catalog {
class DatabaseCatalog;
}  // namespace noisepage::catalog

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

  /**
   * Retrieve the column statistic entry for a particular column from pg_statistic.
   *
   * @param txn                 The transaction to use
   * @param database_catalog    A pointer to the database catalog
   * @param table_oid           The OID of the table containing the column
   * @param col_oid             The OID of the column to retrieve statistics for
   * @return                    Column statistics for the column
   */
  std::unique_ptr<optimizer::ColumnStatsBase> GetColumnStatistics(
      common::ManagedPointer<transaction::TransactionContext> txn,
      common::ManagedPointer<DatabaseCatalog> database_catalog, table_oid_t table_oid, col_oid_t col_oid);

  /**
   * Retrieve all column statistic entries for a particular table from pg_statistic.
   *
   * @param txn                 The transaction to use
   * @param database_catalog    A pointer to the database catalog
   * @param table_oid           The OID of the table
   * @return                    Table statistics for the table
   */
  std::unique_ptr<optimizer::TableStats> GetTableStatistics(common::ManagedPointer<transaction::TransactionContext> txn,
                                                            common::ManagedPointer<DatabaseCatalog> database_catalog,
                                                            table_oid_t table_oid);

  /**
   * Helper method that creates a column statistics object from a projected row
   *
   * @pre Projected row must be initialed with row contents
   *
   * @param all_cols_pr     Projected row from pg_statistic table. Must already be initialized with a row's contents
   * @param table_oid       Table oid that the column belongs to
   * @param col_oid         Column oid of the column
   * @param type            Type id of column
   * @return
   */
  std::unique_ptr<optimizer::ColumnStatsBase> CreateColumnStats(
      common::ManagedPointer<storage::ProjectedRow> all_cols_pr, table_oid_t table_oid, col_oid_t col_oid,
      type::TypeId type);

  /**
   * Helper method that creates a columns statistics object from supplied information
   * @tparam T                  SQL type of the column
   * @param table_oid           Table oid that the column belongs to
   * @param col_oid             Column oid of the column
   * @param num_rows            Number of rows that the column has
   * @param frac_null           Fractions of values that are null in the column
   * @param distinct_values     Number of distinct values in the collum
   * @param top_k_str           Serialized version of TopKElements object or nullptr
   * @param histogram_str       Serialized version of Histogram object or nullptr
   * @param type                Type id of column
   * @return                    Column statistics
   */
  template <typename T>
  std::unique_ptr<optimizer::ColumnStatsBase> CreateColumnStats(
      table_oid_t table_oid, col_oid_t col_oid, size_t num_rows, double frac_null, size_t distinct_values,
      const storage::VarlenEntry *top_k_str, const storage::VarlenEntry *histogram_str, type::TypeId type);

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
