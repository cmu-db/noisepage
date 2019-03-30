#pragma once

#include <string>
#include <vector>
#include "common/spin_latch.h"
#include "common/strong_typedef.h"
#include "storage/write_ahead_log/log_io.h"
#include "storage/write_ahead_log/log_manager.h"
#include "storage/projected_columns.h"
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
      : log_file_path_prefix_(log_file_path_prefix),
        buffer_size_(buffer_size) {}

  /**
   * Start a new checkpoint with the given transaxtion context.
   * @param txn transaction context this checkpoint will be running under.
   */
  void StartCheckpoint(transaction::TransactionContext *txn) {
    txn_ = txn;
    out_.Open((log_file_path_prefix_ + std::to_string(!(txn->StartTime()))).c_str());
    varlen_out_.Open((log_file_path_prefix_ + std::to_string(!(txn->StartTime())) + "_varlen").c_str());
  }

  /**
   * Finish the current checkpoint.
   */
  void EndCheckpoint() {
    out_.Persist();
    out_.Close();
    varlen_out_.Persist();
    varlen_out_.Close();
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
  void Checkpoint(DataTable &table, const storage::BlockLayout &layout);

 private:
  BufferedLogWriter out_;
  BufferedLogWriter varlen_out_;
  std::string log_file_path_prefix_;
  uint32_t varlen_offset_;
  uint32_t buffer_size_;

  transaction::TransactionContext *txn_;

  void SerializeRow(const ProjectedRow *row) {
    uint32_t row_size = row->Size();
    out_.BufferWrite(row, row_size);
  }

  void SerializeVarlen(const VarlenEntry *entry) {
    uint32_t varlen_size = entry->Size();
    TERRIER_ASSERT(varlen_size <= VarlenEntry::InlineThreshold(), "Small varlens should be inlined.");
    WriteValue<uint32_t>(varlen_out_, varlen_size);
    varlen_out_.BufferWrite(entry->Content(), varlen_size);
    // TODO(Mengyang): used a magic number here, because sizeof(uint32_t) produces long unsigned int instead of uint32_t.
    varlen_offset_ += (4 + varlen_size);
  }

  template <class T>
  void WriteValue(BufferedLogWriter &out, const T &val) {
    out.BufferWrite(&val, sizeof(T));
  }
};
}  // namespace terrier::storage