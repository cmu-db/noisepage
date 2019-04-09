#pragma once

#include <string>
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
  explicit CheckpointManager(const char *log_file_path_prefix) : log_file_path_prefix_(log_file_path_prefix) {}

  /**
   * Start a new checkpoint with the given transaxtion context.
   * @param txn transaction context this checkpoint will be running under.
   */
  void StartCheckpoint(transaction::TransactionContext *txn) {
    txn_ = txn;
    out_.Open((GetCheckpointFilePath(txn)).c_str());
  }

  /**
   * Finish the current checkpoint.
   */
  void EndCheckpoint() {
    out_.Persist();
    out_.Close();
    txn_ = nullptr;
  }

  /**
   * Construct the path to the checkpoint file, given the txn context.
   * @param txn context the current checkpoint is running under
   * @return path to the checkpoint file.
   */
  std::string GetCheckpointFilePath(transaction::TransactionContext *txn) {
    return log_file_path_prefix_ + std::to_string(!(txn->StartTime()));
  }

  /**
   * Persist a table. This is achieved by first scan the table with a ProjectedColumn buffer, then transfer the data
   * to a ProjectedRow buffer and write the memory representation of the ProjectedRow directly to disk. Varlen columns
   * that is not inlined will be written to another checkpoint file.
   *
   * TODO(Mengyang): possible optimizations:
   *                 * store projected columns directly to disk
   *                 * use a batch of ProjectedRows as buffer
   *                 * support morsel
   */
  void Checkpoint(const SqlTable &table, const BlockLayout &layout);

  /**
   * Begin a recovery. This will clear all registered tables and layouts.
   */
  void StartRecovery(transaction::TransactionContext *txn) { txn_ = txn; }

  /**
   * Register a table in the checkpoint manager, so that its content can be restored during the recovery.
   * @param table to be recovered.
   * @param layout of the table.
   */
  void RegisterTable(SqlTable *table, BlockLayout *layout) {
    tables_.push_back(table);
    layouts_.push_back(layout);
  }

  /**
   * Read the content of a file, and reinsert all tuples into the tables already registered.
   */
  void Recover(const char *log_file_path);

  /**
   * Stop the current recovery. All registered tables are cleared.
   */
  void EndRecovery() {
    txn_ = nullptr;
    tables_.clear();
    layouts_.clear();
  }

 private:
  std::string log_file_path_prefix_;
  BufferedTupleWriter out_;
  transaction::TransactionContext *txn_ = nullptr;
  std::vector<SqlTable *> tables_;
  std::vector<BlockLayout *> layouts_;

  SqlTable *GetTable(catalog::table_oid_t oid) {
    // TODO(mengyang): add support to multiple tables
    return tables_.at(0);
  }

  BlockLayout *GetLayout(catalog::table_oid_t oid) {
    // TODO(mengyang): add support to multiple tables
    return layouts_.at(0);
  }
};

}  // namespace terrier::storage
