#pragma once

#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "catalog/catalog_defs.h"
#include "storage/recovery/abstract_log_provider.h"
#include "storage/sql_table.h"
#include "transaction/transaction_manager.h"

namespace terrier::storage {

/**
 * TODO(Gus): Replace when catalog is brought in
 * Temporary "catalog" to use for recovery. Maps a database oid to a map that maps table oids to SQL table pointers
 */
using RecoveryCatalog =
    std::unordered_map<catalog::db_oid_t, std::unordered_map<catalog::table_oid_t, storage::SqlTable *>>;

/**
 * Recovery Manager
 * TODO(Gus): Add more documentation when API is finalized
 */
class RecoveryManager {
 public:
  /**
   * @param log_provider arbitrary provider to receive logs from
   * @param catalog system catalog to interface with sql tables
   * @param txn_manager txn manager to use for re-executing recovered transactions
   */
  explicit RecoveryManager(AbstractLogProvider *log_provider, RecoveryCatalog *catalog,
                           transaction::TransactionManager *txn_manager)
      : log_provider_(log_provider), catalog_(catalog), txn_manager_(txn_manager) {}

  /**
   * Recovers the databases using the provided log provider
   */
  void Recover() { RecoverFromLogs(); }

 private:
  FRIEND_TEST(RecoveryTests, SingleTableTest);
  FRIEND_TEST(RecoveryTests, HighAbortRateTest);
  FRIEND_TEST(RecoveryTests, MultiDatabaseTest);

  // Log provider for reading in logs
  AbstractLogProvider *log_provider_;

  // Catalog to fetch table pointers
  RecoveryCatalog *catalog_;

  // Transaction manager to create transactions for recovery
  transaction::TransactionManager *txn_manager_;

  // Used during recovery from log. Maps old tuple slot to new tuple slot
  // TODO(Gus): This map may get huge, benchmark whether this becomes a problem and if we need a more sophisticated data
  // structure
  std::unordered_map<TupleSlot, TupleSlot> tuple_slot_map_;

  // Used during recovery from log. Maps a the txn id from the persisted txn to its changes we have buffered. We buffer
  // changes until commit time. This ensures serializability, and allows us to skip changes from aborted txns.
  std::unordered_map<transaction::timestamp_t, std::vector<std::pair<LogRecord *, std::vector<byte *>>>>
      buffered_changes_map_;

  /**
   * Recovers the databases from the logs.
   * @note this is a separate method so in the future, we can also have a RecoverFromCheckpoint method
   */
  void RecoverFromLogs();

  /**
   * @brief Replays a transaction corresponding to the given log record log record.
   * @param log_record abort or commit record for transaction to replay
   * @param varlen_ptrs pointer to all varlen contents in this log r
   */
  void ReplayTransaction(LogRecord *log_record, std::vector<byte *> varlen_ptrs);

  /**
   * @param db_oid database oid for requested table
   * @param table_oid table oid for requested table
   * @return pointer to requested Sql table
   */
  storage::SqlTable *GetSqlTable(catalog::db_oid_t db_oid, catalog::table_oid_t table_oid) {
    TERRIER_ASSERT(catalog_->find(db_oid) != catalog_->end(), "Database must exist in catalog");
    TERRIER_ASSERT(catalog_->at(db_oid).find(table_oid) != catalog_->at(db_oid).end(), "Table must exist in catalog");
    return catalog_->at(db_oid).at(table_oid);
  }
};
}  // namespace terrier::storage
