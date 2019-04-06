#pragma once

#include <string>
#include <vector>
#include "common/spin_latch.h"
#include "common/strong_typedef.h"
#include "storage/checkpoint_io.h"
#include "storage/write_ahead_log/log_io.h"
#include "storage/projected_columns.h"
#include "storage/sql_table.h"
#include "transaction/transaction_context.h"
#include "transaction/transaction_defs.h"

namespace terrier::storage {

/**
 * A CheckpointManager is responsible for serializing tuples of all tables in the database out and divide time into epochs by doing checkpoint. 
 */
class CheckpointManager {
 public:
  /**
   * Constructs a new CheckpointManager, writing its records out to the given file.
   */
  CheckpointManager(const char *log_file_path_prefix, const uint32_t buffer_size)
      : log_file_path_prefix_(log_file_path_prefix) {}

  /**
   * Start a new checkpoint with the given transaxtion context.
   * @param txn transaction context this checkpoint will be running under.
   */
  void StartCheckpoint(transaction::TransactionContext *txn) {
    txn_ = txn;
    out_.Open((log_file_path_prefix_ + std::to_string(!(txn->StartTime()))).c_str());
  }

  /**
   * Finish the current checkpoint.
   */
  void EndCheckpoint() {
    out_.Persist();
    out_.Close();
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
  void Checkpoint(SqlTable &table, const storage::BlockLayout &layout);

 private:
  std::string log_file_path_prefix_;
  BufferedTupleWriter out_;
  transaction::TransactionContext *txn_;
  
};

}  // namespace terrier::storage