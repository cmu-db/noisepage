#include "transaction/transaction_manager.h"

#include <unordered_set>
#include <utility>

#include "common/scoped_timer.h"
#include "common/thread_context.h"
#include "metrics/metrics_store.h"

namespace terrier::transaction {
TransactionContext *TransactionManager::BeginTransaction() {
  timestamp_t start_time;
  TransactionContext *result;

  const bool txn_metrics_enabled =
      common::thread_context.metrics_store_ != nullptr &&
      common::thread_context.metrics_store_->ComponentToRecord(metrics::MetricsComponent::TRANSACTION);

  // start the operating unit resource tracker
  if (txn_metrics_enabled) common::thread_context.resource_tracker_.Start();
  start_time = timestamp_manager_->BeginTransaction();
  result = new TransactionContext(start_time, start_time + INT64_MIN, buffer_pool_, log_manager_);
  // Ensure we do not return from this function if there are ongoing write commits
  txn_gate_.Traverse();

  if (txn_metrics_enabled) {
    common::thread_context.resource_tracker_.Stop();
    auto &resource_metrics = common::thread_context.resource_tracker_.GetMetrics();
    common::thread_context.metrics_store_->RecordBeginData(resource_metrics);
  }

  return result;
}

void TransactionManager::LogCommit(TransactionContext *const txn, const timestamp_t commit_time,
                                   const callback_fn commit_callback, void *const commit_callback_arg,
                                   const timestamp_t oldest_active_txn) {
  if (log_manager_ != DISABLED) {
    // At this point the commit has already happened for the rest of the system.
    // Here we will manually add a commit record and flush the buffer to ensure the logger
    // sees this record.
    byte *const commit_record = txn->redo_buffer_.NewEntry(storage::CommitRecord::Size());
    storage::CommitRecord::Initialize(commit_record, txn->StartTime(), commit_time, commit_callback,
                                      commit_callback_arg, oldest_active_txn, txn->IsReadOnly(), txn,
                                      timestamp_manager_.Get());
  } else {
    // Otherwise, logging is disabled. We should pretend to have serialized and flushed the record so the rest of the
    // system proceeds correctly
    timestamp_manager_->RemoveTransaction(txn->StartTime());
    commit_callback(commit_callback_arg);
  }
  txn->redo_buffer_.Finalize(true);
}

timestamp_t TransactionManager::UpdatingCommitCriticalSection(TransactionContext *const txn) {
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

  // flip all timestamps to be committed
  for (auto &it : txn->undo_buffer_) it.Timestamp().store(commit_time);
  return commit_time;
}

timestamp_t TransactionManager::Commit(TransactionContext *const txn, transaction::callback_fn callback,
                                       void *callback_arg) {
  timestamp_t result;
  const bool txn_metrics_enabled =
      common::thread_context.metrics_store_ != nullptr &&
      common::thread_context.metrics_store_->ComponentToRecord(metrics::MetricsComponent::TRANSACTION);

  // start the operating unit resource tracker
  if (txn_metrics_enabled) common::thread_context.resource_tracker_.Start();

  TERRIER_ASSERT(!txn->must_abort_,
                 "This txn was marked that it must abort. Set a breakpoint at TransactionContext::MustAbort() to see a "
                 "stack trace for when this flag is getting tripped.");
  result = txn->IsReadOnly() ? timestamp_manager_->CheckOutTimestamp() : UpdatingCommitCriticalSection(txn);

  txn->finish_time_.store(result);

  while (!txn->commit_actions_.empty()) {
    TERRIER_ASSERT(deferred_action_manager_ != DISABLED, "No deferred action manager exists to process actions");
    txn->commit_actions_.front()(deferred_action_manager_.Get());
    txn->commit_actions_.pop_front();
  }

  // If logging is enabled and our txn is not read only, we need to persist the oldest active txn at the time we
  // committed. This will allow us to correctly order and execute transactions during recovery.
  timestamp_t oldest_active_txn = INVALID_TXN_TIMESTAMP;
  if (log_manager_ != DISABLED && !txn->IsReadOnly()) {
    // TODO(Gus): Getting the cached timestamp may cause replication delays, as the cached timestamp is a stale value,
    // so transactions may wait for longer than they need to. We should analyze the impact of this when replication is
    // added.
    oldest_active_txn = timestamp_manager_->CachedOldestTransactionStartTime();
  }
  LogCommit(txn, result, callback, callback_arg, oldest_active_txn);

  // We hand off txn to GC, however, it won't be GC'd until the LogManager marks it as serialized
  if (gc_enabled_) {
    common::SpinLatch::ScopedSpinLatch guard(&timestamp_manager_->curr_running_txns_latch_);
    // It is not necessary to have to GC process read-only transactions, but it's probably faster to call free off
    // the critical path there anyway
    // Also note here that GC will figure out what varlen entries to GC, as opposed to in the abort case.
    completed_txns_.push_front(txn);
  }

  if (txn_metrics_enabled) {
    common::thread_context.resource_tracker_.Stop();
    auto &resource_metrics = common::thread_context.resource_tracker_.GetMetrics();
    common::thread_context.metrics_store_->RecordCommitData(static_cast<uint64_t>(txn->IsReadOnly()), resource_metrics);
  }

  return result;
}

void TransactionManager::LogAbort(TransactionContext *const txn) {
  // We flush the buffer containing an AbortRecord only if this transaction has previously flushed a RedoBuffer. This
  // way the Recovery manager knows to rollback changes for the aborted transaction.
  if (log_manager_ != DISABLED && txn->redo_buffer_.HasFlushed()) {
    // If we are logging the AbortRecord, then the transaction must have previously flushed records, so it must have
    // made updates
    TERRIER_ASSERT(!txn->undo_buffer_.Empty(), "Should not log AbortRecord for read only txn");
    // Here we will manually add an abort record and flush the buffer to ensure the logger
    // sees this record. Because the txn is aborted and will not be recovered, we can discard all the records that
    // currently exist. Only the abort record is needed.
    txn->redo_buffer_.Reset();
    byte *const abort_record = txn->redo_buffer_.NewEntry(storage::AbortRecord::Size());
    storage::AbortRecord::Initialize(abort_record, txn->StartTime(), txn, timestamp_manager_.Get());
    // Signal to the log manager that we are ready to be logged out
    txn->redo_buffer_.Finalize(true);
  } else {
    // Otherwise, logging is disabled or we never flushed, so we can just mark the txns log as processed. We should
    // pretend to have flushed the record so the rest of the system proceeds correctly Discard the redo buffer that is
    // not yet logged out
    txn->redo_buffer_.Finalize(false);
    // Since there is nothing to log, we can mark it as processed
    timestamp_manager_->RemoveTransaction(txn->StartTime());
  }
}

timestamp_t TransactionManager::Abort(TransactionContext *const txn) {
  // Immediately clear the abort actions stack
  while (!txn->abort_actions_.empty()) {
    TERRIER_ASSERT(deferred_action_manager_ != DISABLED, "No deferred action manager exists to process actions");
    txn->abort_actions_.front()(deferred_action_manager_.Get());
    txn->abort_actions_.pop_front();
  }

  // We need to beware not to rollback a version chain multiple times, as that is just wasted computation
  std::unordered_set<storage::TupleSlot> slots_rolled_back;
  for (auto &record : txn->undo_buffer_) {
    auto it = slots_rolled_back.find(record.Slot());
    if (it == slots_rolled_back.end()) {
      slots_rolled_back.insert(record.Slot());
      Rollback(txn, record);
    }
  }

  // Now that the in-place versions have been restored, we need to check out an abort timestamp as well. This serves
  // to force the rest of the system to acknowledge the rollback, lest a reader suffers from an a-b-a problem in the
  // version record
  const timestamp_t abort_time = timestamp_manager_->CheckOutTimestamp();
  // There is no need to flip these timestamps in a critical section, because readers can never see the aborted
  // version either way, unlike in the commit case, where unrepeatable reads may occur.
  for (auto &it : txn->undo_buffer_) it.Timestamp().store(abort_time);
  txn->finish_time_.store(abort_time);
  txn->aborted_ = true;

  // The last update might not have been installed, and thus Rollback would miss it if it contains a
  // varlen entry whose memory content needs to be freed. We have to check for this case manually.
  GCLastUpdateOnAbort(txn);

  LogAbort(txn);

  // We hand off txn to GC, however, it won't be GC'd until the LogManager marks it as serialized
  if (gc_enabled_) {
    common::SpinLatch::ScopedSpinLatch guard(&timestamp_manager_->curr_running_txns_latch_);
    // It is not necessary to have to GC process read-only transactions, but it's probably faster to call free off
    // the critical path there anyway
    // Also note here that GC will figure out what varlen entries to GC, as opposed to in the abort case.
    completed_txns_.push_front(txn);
  }

  return abort_time;
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
  const storage::BlockLayout &layout = redo->GetTupleSlot().GetBlock()->data_table_->GetBlockLayout();
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

TransactionQueue TransactionManager::CompletedTransactionsForGC() {
  common::SpinLatch::ScopedSpinLatch guard(&timestamp_manager_->curr_running_txns_latch_);
  return std::move(completed_txns_);
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
  storage::UndoRecord *undo_record = table->AtomicallyReadVersionPtr(slot, accessor);
  // In a loop, we will need to undo all updates belonging to this transaction. Because we do not unlink undo records,
  // otherwise this ends up being a quadratic operation to rollback the first record not yet rolled back in the chain.
  TERRIER_ASSERT(undo_record != nullptr && undo_record->Timestamp().load() == txn->finish_time_.load(),
                 "Attempting to rollback on a TupleSlot where this txn does not hold the write lock!");
  while (undo_record != nullptr && undo_record->Timestamp().load() == txn->finish_time_.load()) {
    switch (undo_record->Type()) {
      case storage::DeltaRecordType::UPDATE:
        // Re-apply the before image
        for (uint16_t i = 0; i < undo_record->Delta()->NumColumns(); i++) {
          // Need to deallocate any possible varlen.
          DeallocateColumnUpdateIfVarlen(txn, undo_record, i, accessor);
          storage::StorageUtil::CopyAttrFromProjection(accessor, slot, *(undo_record->Delta()), i);
        }
        break;
      case storage::DeltaRecordType::INSERT:
        // Same as update, need to deallocate possible varlens.
        DeallocateInsertedTupleIfVarlen(txn, undo_record, accessor);
        accessor.SetNull(slot, storage::VERSION_POINTER_COLUMN_ID);
        accessor.Deallocate(slot);
        break;
      case storage::DeltaRecordType::DELETE:
        accessor.SetNotNull(slot, storage::VERSION_POINTER_COLUMN_ID);
        break;
      default:
        throw std::runtime_error("unexpected delta record type");
    }
    undo_record = undo_record->Next();
  }
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
  for (uint16_t i = storage::NUM_RESERVED_COLUMNS; i < layout.NumColumns(); i++) {
    storage::col_id_t col_id(i);
    if (layout.IsVarlen(col_id)) {
      auto *varlen = reinterpret_cast<storage::VarlenEntry *>(accessor.AccessWithNullCheck(undo->Slot(), col_id));
      if (varlen != nullptr) {
        if (varlen->NeedReclaim()) txn->loose_ptrs_.push_back(varlen->Content());
      }
    }
  }
}
}  // namespace terrier::transaction
