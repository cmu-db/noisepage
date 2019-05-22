#include "transaction/transaction_manager.h"
#include <algorithm>
#include <queue>
#include <utility>

namespace terrier::transaction {
TransactionContext *TransactionManager::BeginTransaction() {
  // Ensure we do not return from this function if there are ongoing write commits
  common::Gate::ScopedExit gate(&txn_gate_);

  timestamp_t start_time;
  {
    // There is a three-way race that needs to be prevented.  Specifically, we
    // cannot allow both a transaction to commit and the GC to poll for the
    // oldest running transaction in between this transaction acquiring its
    // begin timestamp and getting inserted into the current running
    // transactions list.  Using the current running transactions latch
    // prevents the GC from polling and stops the race.  This allows us to
    // replace acquiring a shared instance of the commit latch with a
    // read-only spin-latch and move the allocation out of a critical section.
    common::SpinLatch::ScopedSpinLatch running_guard(&curr_running_txns_latch_);
    start_time = time_++;

    // TODO(Tianyu):
    // Maybe embed this into the data structure, or use an object pool?
    // Doing this with std::map or other data structure is risky though, as they may not
    // guarantee that the iterator or underlying pointer is stable across operations.
    // (That is, they may change as concurrent inserts and deletes happen)
    const auto ret UNUSED_ATTRIBUTE = curr_running_txns_.emplace(start_time);
    TERRIER_ASSERT(ret.second, "commit start time should be globally unique");
  }  // Release latch on current running transactions

  // Do the allocation outside of any critical section
  auto *const result = new TransactionContext(start_time, start_time + INT64_MIN, buffer_pool_, log_manager_, this);

  return result;
}

void TransactionManager::LogCommit(TransactionContext *const txn, const timestamp_t commit_time,
                                   const callback_fn callback, void *const callback_arg) {
  txn->TxnId().store(commit_time);
  if (log_manager_ != LOGGING_DISABLED) {
    // At this point the commit has already happened for the rest of the system.
    // Here we will manually add a commit record and flush the buffer to ensure the logger
    // sees this record.
    byte *const commit_record = txn->redo_buffer_.NewEntry(storage::CommitRecord::Size());
    const bool is_read_only = txn->undo_buffer_.Empty();
    storage::CommitRecord::Initialize(commit_record, txn->StartTime(), commit_time, callback, callback_arg,
                                      is_read_only, txn);
    // Signal to the log manager that we are ready to be logged out
  } else {
    // Otherwise, logging is disabled. We should pretend to have flushed the record so the rest of the system proceeds
    // correctly
    txn->log_processed_ = true;
    callback(callback_arg);
  }
  txn->redo_buffer_.Finalize(true);
}

timestamp_t TransactionManager::ReadOnlyCommitCriticalSection(TransactionContext *const txn, const callback_fn callback,
                                                              void *const callback_arg) {
  // No records to update. No commit will ever depend on us. We can do all the work outside of the critical section
  const timestamp_t commit_time = time_++;
  // TODO(Tianyu): Notice here that for a read-only transaction, it is necessary to communicate the commit with the
  // LogManager, so speculative reads are handled properly,  but there is no need to actually write out the read-only
  // transaction's commit record to disk.
  LogCommit(txn, commit_time, callback, callback_arg);
  return commit_time;
}

timestamp_t TransactionManager::UpdatingCommitCriticalSection(TransactionContext *const txn, const callback_fn callback,
                                                              void *const callback_arg) {
  // WARNING: This operation has to happen appear atomic to new transactions:
  // transaction 1        transaction 2
  //   begin
  //   write a
  //   commit
  //   add to log             begin
  //                          read a
  //   unlock a               ...
  //                          read a
  //
  //  Transaction 2 will incorrectly read the original version of 'a' the first
  //  time because transaction 1 hasn't made its writes visible and then reads
  //  the correct version the second time, violating snapshot isolation.
  //  Make sure you solve this problem before you remove this gate for whatever reason.
  common::Gate::ScopedLock gate(&txn_gate_);
  const timestamp_t commit_time = time_++;

  LogCommit(txn, commit_time, callback, callback_arg);
  // flip all timestamps to be committed
  for (auto &it : txn->undo_buffer_) it.Timestamp().store(commit_time);

  return commit_time;
}

timestamp_t TransactionManager::Commit(TransactionContext *const txn, transaction::callback_fn callback,
                                       void *callback_arg) {
  const timestamp_t result = txn->undo_buffer_.Empty() ? ReadOnlyCommitCriticalSection(txn, callback, callback_arg)
                                                       : UpdatingCommitCriticalSection(txn, callback, callback_arg);
  while (!txn->commit_actions_.empty()) {
    txn->commit_actions_.front()();
    txn->commit_actions_.pop_front();
  }

  {
    // In a critical section, remove this transaction from the table of running transactions
    common::SpinLatch::ScopedSpinLatch guard(&curr_running_txns_latch_);
    const timestamp_t start_time = txn->StartTime();
    const size_t ret UNUSED_ATTRIBUTE = curr_running_txns_.erase(start_time);
    TERRIER_ASSERT(ret == 1, "Committed transaction did not exist in global transactions table");
    // It is not necessary to have to GC process read-only transactions, but it's probably faster to call free off
    // the critical path there anyway
    // Also note here that GC will figure out what varlen entries to GC, as opposed to in the abort case.
    if (gc_enabled_) completed_txns_.push_front(txn);
  }
  return result;
}

void TransactionManager::Abort(TransactionContext *const txn) {
  // Immediately clear the abort actions stack
  while (!txn->abort_actions_.empty()) {
    txn->abort_actions_.front()();
    txn->abort_actions_.pop_front();
  }

  // no commit latch required here since all operations are transaction-local
  for (auto &it : txn->undo_buffer_) Rollback(txn, it);
  // The last update might not have been installed, and thus Rollback would miss it if it contains a
  // varlen entry whose memory content needs to be freed. We have to check for this case manually.
  GCLastUpdateOnAbort(txn);
  // Discard the redo buffer that is not yet logged out
  txn->redo_buffer_.Finalize(false);
  txn->log_processed_ = true;
  {
    // In a critical section, remove this transaction from the table of running transactions
    common::SpinLatch::ScopedSpinLatch guard(&curr_running_txns_latch_);
    const timestamp_t start_time = txn->StartTime();
    const size_t ret UNUSED_ATTRIBUTE = curr_running_txns_.erase(start_time);
    TERRIER_ASSERT(ret == 1, "Aborted transaction did not exist in global transactions table");
    if (gc_enabled_) completed_txns_.push_front(txn);
  }
}

void TransactionManager::GCLastUpdateOnAbort(TransactionContext *const txn) {
  auto *last_log_record = reinterpret_cast<storage::LogRecord *>(txn->redo_buffer_.LastRecord());
  auto *last_undo_record = reinterpret_cast<storage::UndoRecord *>(txn->undo_buffer_.LastRecord());
  // It is possible that there is nothing to do here, because we aborted for reasons other than a
  // write-write conflict (client calling abort, validation phase failure, etc.). We can
  // tell whether a write-write conflict happened by checking the last entry of the undo to see
  // if the update was indeed installed.
  // TODO(Tianyu): This way of gcing varlen implies that we abort right away on a conflict
  // and not perform any further updates. Shouldn't be a stretch.
  if (last_log_record == nullptr) return;                                     // there are no updates
  if (last_log_record->RecordType() != storage::LogRecordType::REDO) return;  // Only redos need to be gc-ed.

  // Last update can potentially contain a varlen that needs to be gc-ed. We now need to check if it
  // was installed or not.
  auto *redo = last_log_record->GetUnderlyingRecordBodyAs<storage::RedoRecord>();
  TERRIER_ASSERT(redo->GetTupleSlot() == last_undo_record->Slot(),
                 "Last undo record and redo record must correspond to each other");
  if (last_undo_record->Table() != nullptr) return;  // the update was installed and will be handled by the GC

  // We need to free any varlen memory in the last update if the code reaches here
  const storage::BlockLayout &layout = redo->GetDataTable()->accessor_.GetBlockLayout();
  for (uint16_t i = 0; i < redo->Delta()->NumColumns(); i++) {
    // Need to deallocate any possible varlen, as updates may have already been logged out and lost.
    storage::col_id_t col_id = redo->Delta()->ColumnIds()[i];
    if (layout.IsVarlen(col_id)) {
      auto *varlen = reinterpret_cast<storage::VarlenEntry *>(redo->Delta()->AccessWithNullCheck(i));
      if (varlen != nullptr) {
        TERRIER_ASSERT(varlen->NeedReclaim() || varlen->IsInlined(), "Fresh updates cannot be compacted or compressed");
        if (varlen->NeedReclaim()) txn->loose_ptrs_.push_back(varlen->Content());
      }
    }
  }
}

timestamp_t TransactionManager::OldestTransactionStartTime() const {
  common::SpinLatch::ScopedSpinLatch guard(&curr_running_txns_latch_);
  const auto &oldest_txn = std::min_element(curr_running_txns_.cbegin(), curr_running_txns_.cend());
  const timestamp_t result = (oldest_txn != curr_running_txns_.end()) ? *oldest_txn : time_.load();
  return result;
}

TransactionQueue TransactionManager::CompletedTransactionsForGC() {
  common::SpinLatch::ScopedSpinLatch guard(&curr_running_txns_latch_);
  return std::move(completed_txns_);
}

void TransactionManager::DeferAction(Action a) {
  TERRIER_ASSERT(GCEnabled(), "Need GC enabled for deferred actions to be executed.");
  common::SpinLatch::ScopedSpinLatch guard(&deferred_actions_latch_);
  deferred_actions_.push({time_.load(), a});
}

std::queue<std::pair<timestamp_t, Action>> TransactionManager::DeferredActionsForGC() {
  common::SpinLatch::ScopedSpinLatch guard(&deferred_actions_latch_);
  return std::move(deferred_actions_);
}

void TransactionManager::Rollback(TransactionContext *txn, const storage::UndoRecord &record) const {
  // No latch required for transaction-local operation
  storage::DataTable *const table = record.Table();
  if (table == nullptr) {
    // This UndoRecord was never installed in the version chain, so we can skip it
    return;
  }
  const storage::TupleSlot slot = record.Slot();
  const storage::TupleAccessStrategy &accessor = table->accessor_;
  // This is slightly weird because we don't necessarily undo the record given, but a record by this txn at the
  // given slot. It ends up being correct because we call the correct number of rollbacks.
  storage::UndoRecord *const version_ptr = table->AtomicallyReadVersionPtr(slot, accessor);
  TERRIER_ASSERT(version_ptr != nullptr && version_ptr->Timestamp().load() == txn->txn_id_.load(),
                 "Attempting to rollback on a TupleSlot where this txn does not hold the write lock!");

  switch (version_ptr->Type()) {
    case storage::DeltaRecordType::UPDATE:
      // Re-apply the before image
      for (uint16_t i = 0; i < version_ptr->Delta()->NumColumns(); i++) {
        // Need to deallocate any possible varlen.
        DeallocateColumnUpdateIfVarlen(txn, version_ptr, i, accessor);
        storage::StorageUtil::CopyAttrFromProjection(accessor, slot, *(version_ptr->Delta()), i);
      }
      break;
    case storage::DeltaRecordType::INSERT:
      // Same as update, need to deallocate possible varlens.
      DeallocateInsertedTupleIfVarlen(txn, version_ptr, accessor);
      accessor.SetNull(slot, VERSION_POINTER_COLUMN_ID);
      accessor.Deallocate(slot);
      break;
    case storage::DeltaRecordType::DELETE:
      accessor.SetNotNull(slot, VERSION_POINTER_COLUMN_ID);
      break;
    default:
      throw std::runtime_error("unexpected delta record type");
  }

  // Because the garbage collector can be concurrently truncating the version chain, it is important we check
  // that the value we write is not changing.
  storage::UndoRecord *next;
  do {
    next = version_ptr->Next();
    // Remove this delta record from the version chain, effectively releasing the lock. At this point, the tuple
    // has been restored to its original form. No CAS needed since we still hold the write lock at time of the atomic
    // write.
    table->AtomicallyWriteVersionPtr(slot, accessor, next);
    // It is still safe at this point if the garbage collector changes the next record from under us, because
    // as long as the abort function does not return, GC cannot deallocate these stale records. We just need
    // to make sure we get them before returning.
  } while (next != version_ptr->Next());
}

void TransactionManager::DeallocateColumnUpdateIfVarlen(TransactionContext *txn, storage::UndoRecord *undo,
                                                        uint16_t projection_list_index,
                                                        const storage::TupleAccessStrategy &accessor) const {
  const storage::BlockLayout &layout = accessor.GetBlockLayout();
  storage::col_id_t col_id = undo->Delta()->ColumnIds()[projection_list_index];
  if (layout.IsVarlen(col_id)) {
    auto *varlen = reinterpret_cast<storage::VarlenEntry *>(accessor.AccessWithNullCheck(undo->Slot(), col_id));
    if (varlen != nullptr) {
      TERRIER_ASSERT(varlen->NeedReclaim() || varlen->IsInlined(), "Fresh updates cannot be compacted or compressed");
      if (varlen->NeedReclaim()) txn->loose_ptrs_.push_back(varlen->Content());
    }
  }
}

void TransactionManager::DeallocateInsertedTupleIfVarlen(TransactionContext *txn, storage::UndoRecord *undo,
                                                         const storage::TupleAccessStrategy &accessor) const {
  const storage::BlockLayout &layout = accessor.GetBlockLayout();
  for (uint16_t i = NUM_RESERVED_COLUMNS; i < layout.NumColumns(); i++) {
    storage::col_id_t col_id(i);
    if (layout.IsVarlen(col_id)) {
      auto *varlen = reinterpret_cast<storage::VarlenEntry *>(accessor.AccessWithNullCheck(undo->Slot(), col_id));
      if (varlen != nullptr) {
        TERRIER_ASSERT(varlen->NeedReclaim() || varlen->IsInlined(), "Fresh updates cannot be compacted or compressed");
        if (varlen->NeedReclaim()) txn->loose_ptrs_.push_back(varlen->Content());
      }
    }
  }
}
}  // namespace terrier::transaction
