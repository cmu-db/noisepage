#include "transaction/transaction_manager.h"
#include <algorithm>
#include <queue>
#include <unordered_set>
#include <utility>

namespace terrier::transaction {
TransactionContext *TransactionManager::BeginTransaction() {
  timestamp_t start_time = timestamp_manager_->BeginTransaction();
  auto *const result =
      new TransactionContext(start_time, start_time + INT64_MIN, buffer_pool_, deferred_action_manager_, log_manager_);
  // Ensure we do not return from this function if there are ongoing write commits
  common::Gate::ScopedExit gate(&txn_gate_);
  return result;
}

void TransactionManager::LogCommit(TransactionContext *const txn, const timestamp_t commit_time,
                                   const callback_fn callback, void *const callback_arg) {
  txn->TxnId().store(commit_time);
  if (log_manager_ != DISABLED) {
    // At this point the commit has already happened for the rest of the system.
    // Here we will manually add a commit record and flush the buffer to ensure the logger
    // sees this record.
    byte *const commit_record = txn->redo_buffer_.NewEntry(storage::CommitRecord::Size());
    storage::CommitRecord::Initialize(commit_record, txn, callback, callback_arg);
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
  const timestamp_t commit_time = timestamp_manager_->CheckOutTimestamp();
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
  const timestamp_t commit_time = timestamp_manager_->CheckOutTimestamp();

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
    txn->commit_actions_.front()(deferred_action_manager_);
    txn->commit_actions_.pop_front();
  }

  timestamp_manager_->RemoveTransaction(txn->StartTime());

  if (deferred_action_manager_ != DISABLED && version_chain_gc_ != DISABLED) {
    // Register to be unlinked when the records are no longer visible.
    // Because the timestamp may have advanced between now and commit time, we may be overly conservative
    deferred_action_manager_->RegisterDeferredAction([=](timestamp_t oldest_txn) {
      version_chain_gc_->Unlink(txn, oldest_txn);
      return true;
    });
  }
  return result;
}

timestamp_t TransactionManager::Abort(TransactionContext *const txn) {
  // We need to beware not to rollback a version chain multiple times, as that is just wasted computation
  std::unordered_set<storage::TupleSlot> slots_rolled_back;
  for (auto &record : txn->undo_buffer_) {
    auto it = slots_rolled_back.find(record.Slot());
    if (it == slots_rolled_back.end()) {
      slots_rolled_back.insert(record.Slot());
      Rollback(txn, record);
    }
  }
  // The last update might not have been installed, and thus Rollback would miss it if it contains a
  // varlen entry whose memory content needs to be freed. We have to check for this case manually.
  RecaimLastUpdateOnAbort(txn);

  // Now that the in-place versions have been restored, we neeed to check out an abort timestamp as well. This serves
  // to force the rest of the system to acknowledge the rollback, lest a reader suffers from an a-b-a problem in the
  // version record. This timestamp does not need to be unique.
  const timestamp_t abort_time = timestamp_manager_->CurrentTime();
  // There is no need to flip these timestamps in a critical section, because readers can never see the aborted
  // version either way, unlike in the commit case, where unrepeatable reads may occur.
  for (auto &it : txn->undo_buffer_) it.Timestamp().store(abort_time);
  txn->TxnId().store(abort_time);
  txn->aborted_ = true;
  // Discard the redo buffer that is not yet logged out
  txn->redo_buffer_.Finalize(false);
  // Since there is nothing to log, we can mark it as processed
  txn->log_processed_ = true;

  // Clear the abort actions stack
  while (!txn->abort_actions_.empty()) {
    txn->abort_actions_.front()(deferred_action_manager_);
    txn->abort_actions_.pop_front();
  }

  timestamp_manager_->RemoveTransaction(txn->StartTime());

  if (deferred_action_manager_ != DISABLED && version_chain_gc_ != DISABLED) {
    // Register to be unlinked when the records are no longer visible.
    // Because the timestamp may have advanced between now and commit time, we may be overly conservative
    deferred_action_manager_->RegisterDeferredAction(
        [=](timestamp_t oldest_txn) { version_chain_gc_->Unlink(txn, oldest_txn); });
  }

  return abort_time;
}

void TransactionManager::RecaimLastUpdateOnAbort(TransactionContext *const txn) {
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
  const storage::BlockLayout &layout = redo->GetTupleSlot().GetBlock()->data_table_->accessor_.GetBlockLayout();
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

void TransactionManager::Rollback(TransactionContext *txn, const storage::UndoRecord &record) {
  // No latch required for transaction-local operation
  storage::DataTable *const table = record.Table();
  if (table == nullptr) {
    // This UndoRecord was never installed in the version chain, so we can skip it
    return;
  }
  const storage::TupleSlot slot = record.Slot();
  const storage::TupleAccessStrategy &accessor = table->accessor_;
  storage::UndoRecord *undo_record = table->AtomicallyReadVersionPtr(slot, accessor);
  // In a loop, we will need to undo all updates belonging to this transaction. Because we do not unlink undo records,
  // otherwise this ends up being a quadratic operation to rollback the first record not yet rolled back in the chain.
  TERRIER_ASSERT(undo_record != nullptr && undo_record->Timestamp().load() == txn->txn_id_.load(),
                 "Attempting to rollback on a TupleSlot where this txn does not hold the write lock!");
  while (undo_record != nullptr && undo_record->Timestamp().load() == txn->txn_id_.load()) {
    switch (undo_record->Type()) {
      case storage::DeltaRecordType::UPDATE:
        // Re-apply the before image
        for (uint16_t i = 0; i < undo_record->Delta()->NumColumns(); i++) {
          // Need to deallocate any possible varlen.
          DeallocateColumnUpdateIfVarlen(txn, undo_record, i, accessor);
          storage::StorageUtil::CopyAttrFromProjection(accessor, slot, *(undo_record->Delta()), i);
        }
        //      version_ptr->Type() = storage::DeltaRecordType::ABORTED_UPDATE;
        break;
      case storage::DeltaRecordType::INSERT:
        // Same as update, need to deallocate possible varlens.
        DeallocateInsertedTupleIfVarlen(txn, undo_record, accessor);
        accessor.SetNull(slot, VERSION_POINTER_COLUMN_ID);
        accessor.Deallocate(slot);
        //      version_ptr->Type() = storage::DeltaRecordType::ABORTED_INSERT;
        break;
      case storage::DeltaRecordType::DELETE:
        accessor.SetNotNull(slot, VERSION_POINTER_COLUMN_ID);
        //      version_ptr->Type() = storage::DeltaRecordType::ABORTED_DELETE;
        break;
      default:
        throw std::runtime_error("unexpected delta record type");
    }
    undo_record = undo_record->Next();
  }
}

void TransactionManager::DeallocateColumnUpdateIfVarlen(TransactionContext *txn, storage::UndoRecord *undo,
                                                        uint16_t projection_list_index,
                                                        const storage::TupleAccessStrategy &accessor) {
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
                                                         const storage::TupleAccessStrategy &accessor) {
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
