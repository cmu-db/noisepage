#pragma once
#include <forward_list>
#include <utility>
#include <vector>
#include "common/object_pool.h"
#include "common/strong_typedef.h"
#include "storage/data_table.h"
#include "storage/record_buffer.h"
#include "storage/storage_defs.h"
#include "storage/tuple_access_strategy.h"
#include "storage/undo_record.h"
#include "storage/write_ahead_log/log_record.h"
#include "transaction/transaction_util.h"

namespace terrier::storage {
class GarbageCollector;
class LogManager;
}  // namespace terrier::storage

namespace terrier::transaction {
/**
 * A transaction context encapsulates the information kept while the transaction is running
 */
class TransactionContext {
 public:
  /**
   * Constructs a new transaction context. Beware that the buffer pool given must be the same one the log manager uses,
   * if logging is enabled.
   * // TODO(Tianyu): We can terrier assert the above condition, but I need to go figure out friends.
   * @param start the start timestamp of the transaction
   * @param txn_id the id of the transaction, should be larger than all start time and commit time
   * @param buffer_pool the buffer pool to draw this transaction's undo buffer from
   * @param log_manager pointer to log manager in the system, or nullptr, if logging is disabled
   */
  TransactionContext(const timestamp_t start, const timestamp_t txn_id,
                     storage::RecordBufferSegmentPool *const buffer_pool, storage::LogManager *const log_manager)
      : start_time_(start), txn_id_(txn_id), undo_buffer_(buffer_pool), redo_buffer_(log_manager, buffer_pool) {}

  ~TransactionContext() {
    for (const byte *ptr : loose_ptrs_) delete[] ptr;
  }
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
    return storage::UndoRecord::InitializeUpdate(undo_buffer_.NewEntry(size), txn_id_.load(), slot, table, redo);
  }

  /**
   * Reserve space on this transaction's undo buffer for a record to log the insert given
   * @param table pointer to the updated DataTable object
   * @param slot the TupleSlot inserted
   * @return a persistent pointer to the head of a memory chunk large enough to hold the undo record
   */
  storage::UndoRecord *UndoRecordForInsert(storage::DataTable *const table, const storage::TupleSlot slot) {
    byte *result = undo_buffer_.NewEntry(sizeof(storage::UndoRecord));
    return storage::UndoRecord::InitializeInsert(result, txn_id_.load(), slot, table);
  }

  /**
   * Reserve space on this transaction's undo buffer for a record to log the delete given
   * @param table pointer to the updated DataTable object
   * @param slot the TupleSlot being deleted
   * @return a persistent pointer to the head of a memory chunk large enough to hold the undo record
   */
  storage::UndoRecord *UndoRecordForDelete(storage::DataTable *const table, const storage::TupleSlot slot) {
    byte *result = undo_buffer_.NewEntry(sizeof(storage::UndoRecord));
    return storage::UndoRecord::InitializeDelete(result, txn_id_.load(), slot, table);
  }

  /**
   * Expose a record that can hold a change, described by the initializer given, that will be logged out to disk.
   * The change can either be copied into this space, or written in the space and then used to change the DataTable.
   * // TODO(Matt): this isn't ideal for Insert since have to call that first and then log it after have a TupleSlot,
   * but it is safe and correct from WAL standpoint
   * @param table the DataTable that this record changes
   * @param slot the slot that this record changes
   * @param initializer the initializer to use for the underlying record
   * @return pointer to the initialized redo record.
   */
  storage::RedoRecord *StageWrite(storage::DataTable *const table, const storage::TupleSlot slot,
                                  const storage::ProjectedRowInitializer &initializer) {
    uint32_t size = storage::RedoRecord::Size(initializer);
    auto *log_record =
        storage::RedoRecord::Initialize(redo_buffer_.NewEntry(size), start_time_, table, slot, initializer);
    return log_record->GetUnderlyingRecordBodyAs<storage::RedoRecord>();
  }

  /**
   * Initialize a record that logs a delete, that will be logged out to disk
   * @param table the DataTable that this record changes
   * @param slot the slot that this record changes
   */
  void StageDelete(storage::DataTable *const table, const storage::TupleSlot slot) {
    uint32_t size = storage::DeleteRecord::Size();
    storage::DeleteRecord::Initialize(redo_buffer_.NewEntry(size), start_time_, table, slot);
  }

  /**
   * Registers a function so that it will be called when this transaction is rollbacked
   * @param callback the function to be called, must be a function that returns void and takes no parameters
   */
  void RegisterRollBackFunction(transaction::rollback_fn fn, void *callback_arg) {
    rollback_functions_.push_front(std::make_pair(fn, callback_arg));
  }

  /**
   * Executes each function registered as a rollback function.
   */
  void ExecuteRollBackFunctions() {
    for (auto const &[fn, arg] : rollback_functions_) {
      fn(arg);
    }
  }

 private:
  friend class storage::GarbageCollector;
  friend class TransactionManager;
  friend class storage::LogManager;
  const timestamp_t start_time_;
  std::atomic<timestamp_t> txn_id_;
  storage::UndoBuffer undo_buffer_;
  storage::RedoBuffer redo_buffer_;
  // TODO(Tianyu): Maybe not so much of a good idea to do this. Make explicit queue in GC?
  //
  std::vector<const byte *> loose_ptrs_;
  // log manager will set this to be true when log records are processed (not necessarily flushed, but will not be read
  // again in the future), so it can be garbage-collected safely.
  bool log_processed_ = false;

  // Functions to be invoked on rollback
  std::forward_list<std::pair<transaction::rollback_fn, void *>> rollback_functions_;
};
}  // namespace terrier::transaction
