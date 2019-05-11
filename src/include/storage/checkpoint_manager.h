#pragma once

#include <dirent.h>
#include <cstdio>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include "common/spin_latch.h"
#include "common/strong_typedef.h"
#include "storage/checkpoint_io.h"
#include "storage/projected_columns.h"
#include "storage/sql_table.h"
#include "storage/write_ahead_log/log_io.h"
#include "transaction/transaction_context.h"
#include "transaction/transaction_defs.h"

namespace terrier::storage {

/**
 * A CheckpointManager is responsible for serializing tuples of all tables in the database out and
 * divide time into epochs by doing checkpoint.
 */
class CheckpointManager {
 public:
  /**
   * Constructs a new CheckpointManager, writing its records out to the given file.
   */
  explicit CheckpointManager(const char *path_prefix) : checkpoint_file_path_prefix_(path_prefix) {}

  /**
   * Manages the lifecycle of a checkpoint. This should be the main entry for checkpointing, and should be protected
   * inside a transaction.
   * Table and schema are temporary, for test purposes only. They should be fetched from catalogs.
   * @param txn
   * @param table
   * @param schema
   */
  void Process(transaction::TransactionContext *txn, const SqlTable &table, const catalog::Schema &schema) {
    StartCheckpoint(txn);
    // TODO(zhaozhes): This should actually iterate through all tables, using catalog information
    Checkpoint(table, schema);
    EndCheckpoint();
  }

  /**
   * Start a new checkpoint with the given transaction context.
   * @param txn transaction context this checkpoint will be running under.
   */
  void StartCheckpoint(transaction::TransactionContext *txn) {
    txn_ = txn;
    // Use a temporary name, and change to correct name once finished. To prevent crashes during checkpointing.
    out_.Open(unfinished_checkpoint_name_);
    // TODO(zhaozhes): persist catalog, get metadata for each table and prepare to checkpoint each table
  }

  /**
   * Persist a table. This is achieved by first scan the table with a ProjectedColumn buffer, then transfer the data
   * to a ProjectedRow buffer and write the memory representation of the ProjectedRow directly to disk. Varlen columns
   * that is not inlined will be written to another checkpoint file.
   *
   * Also, the type of the catalog table (if it is a catalog table) is required for recovery proccedure.
   *
   * TODO(Mengyang): possible optimizations:
   *                 * store projected columns directly to disk
   *                 * use a batch of ProjectedRows as buffer
   *                 * support morsel
   *
   * TODO(Mengyang): Currently we require a schema passed in, but this is wrong especially with multi-version schema.
   *  This is no longer required once the catalog is merged, since we can get the schema of a table from the catalog.
   *
   * @param table SqlTable to be checkpointed
   * @param schema of the table to becheckpointed
   * @param catalog_type 0 if this table is not a catalog table, otherwise, this is the type of the catalog table.
   */
  void Checkpoint(const SqlTable &table, const catalog::Schema &schema, uint32_t catalog_type = 0);

  /**
   * Finish the current checkpoint.
   */
  void EndCheckpoint() {
    out_.Persist();
    out_.Close();
    std::rename(unfinished_checkpoint_name_, GetCheckpointFilePath(txn_).c_str());
    txn_ = nullptr;
  }

  /**
   * Construct the path to the checkpoint file, given the txn context.
   * @param txn context the current checkpoint is running under
   * @return path to the checkpoint file.
   */
  std::string GetCheckpointFilePath(transaction::TransactionContext *txn) {
    return checkpoint_file_path_prefix_ + std::to_string(!(txn->StartTime()));
  }

  /**
   * Get the most up to date checkpoint file name
   * @return path to the latest checkpoint file (with largest transaction id)
   */
  std::pair<std::string, terrier::transaction::timestamp_t> GetLatestCheckpointFilename() {
    char const *path = ".";
    std::string file_name;
    auto largest_timestamp = static_cast<terrier::transaction::timestamp_t>(0);

    DIR *dir;
    struct dirent *ent;
    if ((dir = opendir(path)) != nullptr) {
      /* print all the files and directories within directory */
      while ((ent = readdir(dir)) != nullptr) {
        std::string candidate(ent->d_name);
        if (candidate.find(checkpoint_file_path_prefix_) == 0) {
          auto timestamp = static_cast<terrier::transaction::timestamp_t>(
              std::stoull(candidate.substr(checkpoint_file_path_prefix_.length(), -1)));
          if (timestamp > largest_timestamp) {
            file_name = candidate;
            largest_timestamp = timestamp;
          }
        }
      }
      closedir(dir);
    } else {
      /* could not open directory */
      throw std::runtime_error("cannot open checkpoint directory");
    }
    return std::make_pair(file_name, largest_timestamp);
  }

  /**
   * Delete all checkpoint files, mainly for test purposes.
   */
  void UnlinkCheckpointFiles() {
    char const *path = ".";
    DIR *dir;
    struct dirent *ent;
    if ((dir = opendir(path)) != nullptr) {
      /* print all the files and directories within directory */
      while ((ent = readdir(dir)) != nullptr) {
        std::string checkpoint_file(ent->d_name);
        if (checkpoint_file.find(checkpoint_file_path_prefix_) == 0) {
          unlink(checkpoint_file.c_str());
        }
      }
      // delete temporary uncompleted files, if any (should not have happened if the system did not crash during test.
      unlink(unfinished_checkpoint_name_);
      closedir(dir);
    } else {
      /* could not open directory */
      throw std::runtime_error("cannot open checkpoint directory");
    }
  }

  /**
   * Begin a recovery. This will clear all registered tables.
   */
  void StartRecovery(transaction::TransactionContext *txn) {
    txn_ = txn;
    tuple_slot_map_.clear();
    tables_.clear();
  }

  /**
   * Register a table in the checkpoint manager, so that its content can be restored during the recovery.
   * @param table to be recovered.
   */
  void RegisterTable(SqlTable *table) { tables_[table->Oid()] = table; }

  /**
   * Called after RegisterTable. Read the content of a file, and reinsert all tuples into the tables already registered.
   */
  void Recover(const char *checkpoint_file_path);

  /**
   * Should be called after Recover(), after the tables are registered and recovered. This function will replay logs
   * from the timestamp and recover all the logs. The tuple_slot_map_ is built during Recover() phase. If there are
   * no checkpoints, this function will recover blank tables from scratch.
   *
   *
   *
   * @param log_file_path log file path.
   * @param checkpoint_timestamp The checkpoint timestamp. All logs with smaller timestamps will be ignored.
   */
  void RecoverFromLogs(const char *log_file_path, terrier::transaction::timestamp_t checkpoint_timestamp);

  // Used in log_test, so put in public
  // TODO(zhaozhes): API should be refactored when oid is no longer hard-coded to 0. Should return oid as well
  // to identify the table to redo.
  /**
   * Read next log record from a log file.
   * Used in checkpoint manager as well as log_test, so put as public function.
   * TODO(zhaozhes): API should be refactored when oid is no longer hard-coded to 0. Should return oid as well
   * to identify the table to redo.
   * @param in Reader.
   * @param varlen_contents A vector used for returning varlen contents. It is necessary for cleaning up memory.
   * The caller will judge whether the pointers should be freed.
   * @return A log record.
   */
  storage::LogRecord *ReadNextLogRecord(storage::BufferedLogReader *in, std::vector<byte *> *varlen_contents);

 private:
  std::string checkpoint_file_path_prefix_;
  const char *unfinished_checkpoint_name_ = "checkpoint.tmp";
  BufferedTupleWriter out_;
  transaction::TransactionContext *txn_ = nullptr;
  std::unordered_map<catalog::table_oid_t, SqlTable *> tables_;
  std::unordered_map<TupleSlot, TupleSlot> tuple_slot_map_;

  SqlTable *GetTable(catalog::table_oid_t oid) {
    // TODO(mengyang): add support to multiple tables
    return tables_.at(oid);
  }
};

}  // namespace terrier::storage
