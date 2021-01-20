#pragma once

#include "catalog/catalog_defs.h"
#include "catalog/postgres/pg_statistic.h"
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
   * @brief Prepare to create pg_language.
   *
   * Does NOT create anything until the relevant bootstrap functions are called.
   *
   * @param db_oid          The OID of the database that pg_language should be created in.
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
   * Add entry to pg_statistic.
   *
   * Currently, this is called inside CreateTableEntry so that each row in pg_attribute (corresponding to a column
   * of a table) has a corresponding row in pg_statistic.
   *
   * @tparam Column type of column (IndexSchema::Column or Schema::Column)
   * @param txn txn to use
   * @param class_oid oid of table or index
   * @param col column to insert
   * @param default_val default value
   * @return whether insertion is successful
   */
  template <typename Column, typename ClassOid, typename ColOid>
  void CreateColumnStatistic(common::ManagedPointer<transaction::TransactionContext> txn, ClassOid class_oid,
                             ColOid col_oid, const Column &col);

  /**
   * Delete entry from pg_statistic.
   * @param txn txn to use
   * @param class_oid oid of table or index
   * @return whether deletion was successful
   */
  template <typename Column, typename ClassOid>
  bool DeleteColumnStatistics(common::ManagedPointer<transaction::TransactionContext> txn, ClassOid class_oid);

  const db_oid_t db_oid_;

  /**
   * The table and indexes that define pg_statistic.
   * Created by: Builder::CreateDatabaseCatalog.
   * Cleaned up by: DatabaseCatalog::TearDown, where the scans from pg_class and pg_index pick these up.
   */
  ///@{
  common::ManagedPointer<storage::SqlTable> statistics_;
  common::ManagedPointer<storage::index::Index> statistics_oid_index_;  // indexed on class OID and column OID
  storage::ProjectedRowInitializer pg_statistic_all_cols_pri_;
  storage::ProjectionMap pg_statistic_all_cols_prm_;
  storage::ProjectedRowInitializer delete_statistics_pri_;
  storage::ProjectionMap delete_statistics_prm_;
  ///@}
};

}  // namespace noisepage::catalog::postgres
