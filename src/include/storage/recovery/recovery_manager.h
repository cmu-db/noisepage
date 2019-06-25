#pragma once

#include <string>
#include <unordered_map>
#include <vector>

#include "catalog/catalog_defs.h"
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
  explicit RecoveryManager(std::string log_file_path, RecoveryCatalog *catalog,
                           transaction::TransactionManager *txn_manager)
      : log_file_path_(std::move(log_file_path)), catalog_(catalog), txn_manager_(txn_manager) {}

  /**
   * Recovers the databases from the provided log file path.
   */
  void Recover() { RecoverFromLogs(); }

 private:
  FRIEND_TEST(RecoveryTests, SingleTableTest);
  FRIEND_TEST(RecoveryTests, HighAbortRateTest);
  FRIEND_TEST(RecoveryTests, MultiDatabaseTest);
  // Path to log file
  std::string log_file_path_;

  // Catalog to fetch table pointers
  RecoveryCatalog *catalog_;

  // Transaction manager to create transactions for recovery
  transaction::TransactionManager *txn_manager_;

  // Used during recovery from log. Maps old tuple slot to new tuple slot
  // TODO(Gus): This map may get huge, benchmark whether this becomes a problem and if we need a more sophisticated data
  // structure
  std::unordered_map<TupleSlot, TupleSlot> tuple_slot_map_;

  // Used during recovery from log. Maps a the txn id from the persisted txn to its changes we have buffered. We buffer changes until commit time. This ensures serializability, and allows us to skip changes from aborted txns.
  std::unordered_map<transaction::timestamp_t, std::vector<LogRecord *>> buffered_changes_map_;

  /**
   * Recovers the databases from the logs.
   * @note this is a separate method so in the future, we can also have a RecoverFromCheckpoint method
   */
  void RecoverFromLogs();

  /**
   * Reads in the next log record from the buffered log reader
   * @param in buffered reader for the log file
   * @return next log record in the log file
   */
  storage::LogRecord *ReadNextRecord(storage::BufferedLogReader *in);

  /**
   * @brief Replays a log record.
   * If the replay suceeds, then the function will return true, and the caller is in charge of cleaning up the log
   * record. If the replay fails, the the function will return false, and the caller should buffer the log record to
   * apply it later.
   * @param log_record record to replay
   */
  void ReplayRecord(LogRecord *log_record);

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
