#pragma once
#include <vector>
#include "common/object_pool.h"
#include "common/typedefs.h"
#include "storage/delta_record.h"
#include "storage/record_buffer.h"
#include "storage/storage_defs.h"
#include "storage/tuple_access_strategy.h"

namespace terrier::storage {
class DataTable;
}

namespace terrier::transaction {
/**
 * A transaction context encapsulates the information kept while the transaction is running
 */
class TransactionContext {
 public:
  /**
   * Constructs a new transaction context
   * @param start the start timestamp of the transaction
   * @param txn_id the id of the transaction, should be larger than all start time and commit time
   * @param buffer_pool the buffer pool to draw this transaction's undo buffer from
   */
  TransactionContext(const timestamp_t start, const timestamp_t txn_id,
                     common::ObjectPool<storage::BufferSegment> *const buffer_pool)
      : start_time_(start), txn_id_(txn_id), undo_buffer_(buffer_pool) {}

  /**
   * @return start time of this transaction
   */
  timestamp_t StartTime() const { return start_time_; }

  /**
   * @return id of this transaction
   */
  const std::atomic<timestamp_t> &TxnId() const { return txn_id_; }

  /**
   * @return id of this transaction
   */
  std::atomic<timestamp_t> &TxnId() { return txn_id_; }

  /**
   * @return the undo buffer of this transaction
   */
  storage::UndoBuffer &GetUndoBuffer() { return undo_buffer_; }

  /**
   * Reserve space on this transaction's undo buffer for a record to log the update given
   * @param table pointer to the updated DataTable object
   * @param slot the TupleSlot being updated
   * @param redo the content of the update
   * @return a persistent pointer to the head of a memory chunk large enough to hold the undo record
   */
  storage::UndoRecord *UndoRecordForUpdate(storage::DataTable *const table, const storage::TupleSlot slot,
                                           const storage::ProjectedRow &redo) {
    uint32_t size = storage::UndoRecord::Size(redo);
    storage::UndoRecord *result = undo_buffer_.NewEntry(size);
    return storage::UndoRecord::InitializeRecord(result, txn_id_.load(), slot, table, redo);
  }

  /**
   * Reserve space on this transaction's undo buffer for a record to log the insert given
   * @param table pointer to the updated DataTable object
   * @param layout the layout of the insert target
   * @param slot the TupleSlot being updated
   * @return a persistent pointer to the head of a memory chunk large enough to hold the undo record
   */
  storage::UndoRecord *UndoRecordForInsert(storage::DataTable *const table, const storage::BlockLayout &layout,
                                           const storage::TupleSlot slot) {
    // TODO(Tianyu): Remove magic constant
    // Pretty sure we want 1, the primary key column?
    uint32_t size = storage::UndoRecord::Size(layout, {1});
    storage::UndoRecord *result = undo_buffer_.NewEntry(size);
    return storage::UndoRecord::InitializeRecord(result, txn_id_.load(), slot, table, layout, {1});
  }

 private:
  const timestamp_t start_time_;
  std::atomic<timestamp_t> txn_id_;
  storage::UndoBuffer undo_buffer_;
};
}  // namespace terrier::transaction
