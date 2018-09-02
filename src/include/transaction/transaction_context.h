#pragma once
#include <vector>
#include "common/object_pool.h"
#include "common/typedefs.h"
#include "storage/data_table.h"
#include "storage/log_record.h"
#include "storage/record_buffer.h"
#include "storage/storage_defs.h"
#include "storage/tuple_access_strategy.h"
#include "storage/undo_record.h"
#include "transaction/transaction_util.h"

namespace terrier::storage {
class GarbageCollector;
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
                     storage::RecordBufferSegmentPool *const buffer_pool, storage::LogManager *log_manager)
      : start_time_(start), txn_id_(txn_id), undo_buffer_(buffer_pool), redo_buffer_(log_manager) {}

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
   * Reserve space on this transaction's undo buffer for a record to log the update given
   * @param table pointer to the updated DataTable object
   * @param slot the TupleSlot being updated
   * @param redo the content of the update
   * @return a persistent pointer to the head of a memory chunk large enough to hold the undo record
   */
  storage::UndoRecord *UndoRecordForUpdate(storage::DataTable *const table, const storage::TupleSlot slot,
                                           const storage::ProjectedRow &redo) {
    const uint32_t size = storage::UndoRecord::Size(redo);
    return storage::UndoRecord::Initialize(undo_buffer_.NewEntry(size), txn_id_.load(), slot, table, redo);
  }

  /**
   * Reserve space on this transaction's undo buffer for a record to log the insert given
   * @param table pointer to the updated DataTable object
   * @param slot the TupleSlot being updated
   * @param insert_record_initializer ProjectedRowInitializer used to initialize an insert undo record
   * @return a persistent pointer to the head of a memory chunk large enough to hold the undo record
   */
  storage::UndoRecord *UndoRecordForInsert(storage::DataTable *const table, const storage::TupleSlot slot,
                                           const storage::ProjectedRowInitializer &insert_record_initializer) {
    byte *result = undo_buffer_.NewEntry(storage::UndoRecord::Size(insert_record_initializer));
    return storage::UndoRecord::Initialize(result, txn_id_.load(), slot, table, insert_record_initializer);
  }

  // TODO(Tianyu): this sort of implies that we will need to take in a SqlTable pointer for undo as well,
  // if we stick with the data table / sql table separation.
  // (Or at least if we stick with it and put index in sql table.)
  storage::RedoRecord *StageWrite(execution::SqlTable *table, tuple_id_t tuple_id,
                                  const storage::ProjectedRowInitializer &initializer) {
    // TODO(Tianyu): Is failing the right thing to do?
    TERRIER_ASSERT(!redo_buffer_.LoggingDisabled(), "Cannot stage a write if logging is disabled");
    uint32_t size = storage::RedoRecord::Size(initializer);
    auto *log_record =
        storage::RedoRecord::Initialize(redo_buffer_.NewEntry(size), start_time_, table, tuple_id, initializer);
    return log_record->GetUnderlyingRecordBodyAs<storage::RedoRecord>();
  }

 private:
  friend class storage::GarbageCollector;
  friend class TransactionManager;
  const timestamp_t start_time_;
  std::atomic<timestamp_t> txn_id_;
  storage::UndoBuffer undo_buffer_;
  storage::RedoBuffer redo_buffer_;
};
}  // namespace terrier::transaction
