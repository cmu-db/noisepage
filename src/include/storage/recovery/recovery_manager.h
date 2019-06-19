#pragma once

#include <storage/sql_table.h>
#include <unordered_map>
#include "catalog/catalog_defs.h"
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
      : log_file_path_(log_file_path), catalog_(catalog), txn_manager_(txn_manager) {}

  /**
   * Recovers the databases from the provided log file path.
   */
  void Recover() { RecoverFromLogs(); }

 private:
  // Path to log file
  std::string log_file_path_;

  // Catalog to fetch table pointers
  RecoveryCatalog *catalog_;

  // Transaction manager to create transactions for recovery
  transaction::TransactionManager *txn_manager_;

  // Used during recovery from log. Maps old tuple slot to new tuple slot
  std::unordered_map<TupleSlot, TupleSlot> tuple_slot_map_;

  // Used during recovery from log. Maps txn id to transaction object
  std::unordered_map<transaction::timestamp_t, transaction::TransactionContext *> txn_map_;

  // Used during recovery from log. Maps a txn id to its changes we have buffered. When a change fails due to a
  // write-write conflict, we must buffer it's changes until the other txn that caused the conflict aborts, or we abort.
  // Needing to buffer changes is rare because in the average case, an aborting txn will never flush its changes to the
  // log
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
   * @return true if record was succesfully replayed, otherwise false
   *
   */
  bool ReplayRecord(LogRecord *log_record);
};
}  // namespace terrier::storage