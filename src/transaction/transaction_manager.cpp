#include "transaction/transaction_manager.h"
#include <utility>

namespace terrier::transaction {
TransactionContext *TransactionManager::BeginTransaction() {
  timestamp_t start_time;
  {
    // In a short critical section, fetch a timestamp while making sure no other transactions are committing
    common::SharedLatch::ScopedSharedLatch guard(&commit_latch_);
    start_time = time_++;
  }

  // TODO(Tianyu):
  // Maybe embed this into the data structure, or use an object pool?
  // Doing this with std::map or other data structure is risky though, as they may not
  // guarantee that the iterator or underlying pointer is stable across operations.
  // (That is, they may change as concurrent inserts and deletes happen)
  auto *result = new TransactionContext(start_time, start_time + INT64_MIN, buffer_pool_, log_manager_);
  table_latch_.Lock();
  auto ret UNUSED_ATTRIBUTE = curr_running_txns_.emplace(result->StartTime(), result);
  TERRIER_ASSERT(ret.second, "commit start time should be globally unique");
  table_latch_.Unlock();
  return result;
}

timestamp_t TransactionManager::Commit(TransactionContext *const txn, transaction::callback_fn callback,
                                       void *callback_arg) {
  // TODO(Tianyu): Potentially don't need to get a commit time for read-only txns
  timestamp_t commit_time;
  {
    // In a critical section where no new transactions are allowed to begin, flip all timestamps to be committed
    common::SharedLatch::ScopedExclusiveLatch guard(&commit_latch_);
    commit_time = time_++;
    for (auto &it : txn->undo_buffer_) it.Timestamp().store(commit_time);
  }

  txn->TxnId().store(commit_time);
  // TODO(Tianyu): Notice here that for a read-only transaction, it is necessary to communicate the commit with the
  // LogManager, so speculative reads are handled properly,  but there is no need to actually write out the read-only
  // transaction's commit record to disk.
  if (log_manager_ != LOGGING_DISABLED) {
    // At this point the commit has already happened for the rest of the system.
    // Here we will manually add a commit record and flush the buffer to ensure the logger
    // sees this record.
    byte *commit_record = txn->redo_buffer_.NewEntry(storage::CommitRecord::Size());
    storage::CommitRecord::Initialize(commit_record, txn->StartTime(), commit_time, callback, callback_arg);
  } else {
    // TODO(Tianyu): Is this the right thing to do?
    callback(callback_arg);
  }
  // Signal to the log manager that we are ready to be logged out
  txn->redo_buffer_.Finalize(true);

  {
    // In a critical section, remove this transaction from the table of running transactions
    common::SpinLatch::ScopedSpinLatch guard(&table_latch_);
    const timestamp_t start_time = txn->StartTime();
    size_t result UNUSED_ATTRIBUTE = curr_running_txns_.erase(start_time);
    TERRIER_ASSERT(result == 1, "Committed transaction did not exist in global transactions table");
    if (gc_enabled_) completed_txns_.push_front(txn);
  }


  return commit_time;
}

void TransactionManager::Abort(TransactionContext *const txn) {
  // no commit latch required on undo since all operations are transaction-local
  timestamp_t txn_id = txn->TxnId().load();  // will not change
  for (auto &it : txn->undo_buffer_) Rollback(txn_id, it);
  // Discard the redo buffer that is not yet logged out
  txn->redo_buffer_.Finalize(false);
  {
    // In a critical section, remove this transaction from the table of running transactions
    common::SpinLatch::ScopedSpinLatch guard(&table_latch_);
    const timestamp_t start_time = txn->StartTime();
    size_t ret UNUSED_ATTRIBUTE = curr_running_txns_.erase(start_time);
    TERRIER_ASSERT(ret == 1, "Aborted transaction did not exist in global transactions table");
    if (gc_enabled_) completed_txns_.push_front(txn);
  }

}

timestamp_t TransactionManager::OldestTransactionStartTime() const {
  common::SpinLatch::ScopedSpinLatch guard(&table_latch_);
  auto oldest_txn = curr_running_txns_.begin();
  timestamp_t result = (oldest_txn != curr_running_txns_.end()) ? oldest_txn->second->StartTime() : time_.load();
  return result;
}

TransactionQueue TransactionManager::CompletedTransactionsForGC() {
  common::SpinLatch::ScopedSpinLatch guard(&table_latch_);
  TransactionQueue hand_to_gc(std::move(completed_txns_));
  TERRIER_ASSERT(completed_txns_.empty(), "TransactionManager's queue should now be empty.");
  return hand_to_gc;
}

void TransactionManager::Rollback(const timestamp_t txn_id, const storage::UndoRecord &record) const {
  // No latch required for transaction-local operation
  storage::DataTable *const table = record.Table();
  if (table == nullptr) {
    // This UndoRecord was never installed in the version chain, so we can skip it
    return;
  }
  const storage::TupleSlot slot = record.Slot();
  // This is slightly weird because we don't necessarily undo the record given, but a record by this txn at the
  // given slot. It ends up being correct because we call the correct number of rollbacks.
  storage::UndoRecord *const version_ptr = table->AtomicallyReadVersionPtr(slot, table->accessor_);
  TERRIER_ASSERT(version_ptr != nullptr && version_ptr->Timestamp().load() == txn_id,
                 "Attempting to rollback on a TupleSlot where this txn does not hold the write lock!");
  switch (version_ptr->Type()) {
    case storage::DeltaRecordType::UPDATE:
      // Re-apply the before image
      for (uint16_t i = 0; i < version_ptr->Delta()->NumColumns(); i++)
        storage::StorageUtil::CopyAttrFromProjection(table->accessor_, slot, *(version_ptr->Delta()), i);
      break;
    case storage::DeltaRecordType::INSERT:table->accessor_.SetNull(slot, VERSION_POINTER_COLUMN_ID);
      break;
    case storage::DeltaRecordType::DELETE:table->accessor_.SetNotNull(slot, VERSION_POINTER_COLUMN_ID);
  }
  // Remove this delta record from the version chain, effectively releasing the lock. At this point, the tuple
  // has been restored to its original form. No CAS needed since we still hold the write lock at time of the atomic
  // write.
  table->AtomicallyWriteVersionPtr(slot, table->accessor_, version_ptr->Next());
}
}  // namespace terrier::transaction
