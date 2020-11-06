#include "storage/garbage_collector.h"

#include <unordered_set>
#include <utility>

#include "common/macros.h"
#include "common/thread_context.h"
#include "loggers/storage_logger.h"
#include "metrics/metrics_store.h"
#include "storage/access_observer.h"
#include "storage/data_table.h"
#include "storage/index/index.h"
#include "transaction/deferred_action_manager.h"
#include "transaction/transaction_context.h"
#include "transaction/transaction_defs.h"
#include "transaction/transaction_manager.h"
#include "transaction/transaction_util.h"

namespace noisepage::storage {

GarbageCollector::GarbageCollector(
    const common::ManagedPointer<transaction::TimestampManager> timestamp_manager,
    const common::ManagedPointer<transaction::DeferredActionManager> deferred_action_manager,
    const common::ManagedPointer<transaction::TransactionManager> txn_manager, AccessObserver *observer)
    : timestamp_manager_(timestamp_manager),
      deferred_action_manager_(deferred_action_manager),
      txn_manager_(txn_manager),
      observer_(observer),
      last_unlinked_{0} {
  NOISEPAGE_ASSERT(txn_manager_->GCEnabled(),
                   "The TransactionManager needs to be instantiated with gc_enabled true for GC to work!");
}

std::pair<uint32_t, uint32_t> GarbageCollector::PerformGarbageCollection() {
  const bool gc_metrics_enabled =
      common::thread_context.metrics_store_ != nullptr &&
      common::thread_context.metrics_store_->ComponentToRecord(metrics::MetricsComponent::GARBAGECOLLECTION);

  if (observer_ != nullptr) observer_->ObserveGCInvocation();
  timestamp_manager_->CheckOutTimestamp();
  const transaction::timestamp_t oldest_txn = timestamp_manager_->OldestTransactionStartTime();
  uint32_t txns_deallocated = ProcessDeallocateQueue(oldest_txn);
  STORAGE_LOG_TRACE("GarbageCollector::PerformGarbageCollection(): txns_deallocated: {}", txns_deallocated);
  uint32_t txns_unlinked, buffer_unlinked, readonly_unlinked;
  std::tie(txns_unlinked, buffer_unlinked, readonly_unlinked) = ProcessUnlinkQueue(oldest_txn);
  STORAGE_LOG_TRACE("GarbageCollector::PerformGarbageCollection(): txns_unlinked: {}", txns_unlinked);
  if (txns_unlinked > 0) {
    // Only update this field if we actually unlinked anything, otherwise we're being too conservative about when it's
    // safe to deallocate the transactions in our queue.
    last_unlinked_ = timestamp_manager_->CheckOutTimestamp();
  }
  STORAGE_LOG_TRACE("GarbageCollector::PerformGarbageCollection(): last_unlinked_: {}",
                    last_unlinked_.UnderlyingValue());
  ProcessDeferredActions(oldest_txn);
  ProcessIndexes();

  if ((txns_deallocated > 0 || txns_unlinked > 0) && gc_metrics_enabled) {
    if (common::thread_context.resource_tracker_.IsRunning()) {
      // Stop the resource tracker for this operating unit
      common::thread_context.resource_tracker_.Stop();
      auto &resource_metrics = common::thread_context.resource_tracker_.GetMetrics();
      common::thread_context.metrics_store_->RecordGCData(txns_deallocated, txns_unlinked, buffer_unlinked,
                                                          readonly_unlinked, gc_interval_, resource_metrics);
    }
    common::thread_context.resource_tracker_.Start();
  }

  return std::make_pair(txns_deallocated, txns_unlinked);
}

uint32_t GarbageCollector::ProcessDeallocateQueue(transaction::timestamp_t oldest_txn) {
  uint32_t txns_processed = 0;

  if (transaction::TransactionUtil::NewerThan(oldest_txn, last_unlinked_)) {
    // All of the transactions in my deallocation queue were unlinked before the oldest running txn in the system, and
    // have been serialized by the log manager. We are now safe to deallocate these txns because no running
    // transaction should hold a reference to them anymore
    for (auto &txn : txns_to_deallocate_) {
      delete txn;
      txns_processed++;
    }
    txns_to_deallocate_.clear();
  }

  return txns_processed;
}

std::tuple<uint32_t, uint32_t, uint32_t> GarbageCollector::ProcessUnlinkQueue(transaction::timestamp_t oldest_txn) {
  transaction::TransactionContext *txn = nullptr;

  // Get the completed transactions from the TransactionManager
  transaction::TransactionQueue completed_txns = txn_manager_->CompletedTransactionsForGC();
  if (!completed_txns.empty()) {
    // Append to our local unlink queue
    txns_to_unlink_.splice_after(txns_to_unlink_.cbefore_begin(), std::move(completed_txns));
  }

  uint32_t txns_processed = 0, buffer_processed = 0, readonly_processed = 0;
  // Certain transactions might not be yet safe to gc. Need to requeue them
  transaction::TransactionQueue requeue;
  // It is sufficient to truncate each version chain once in a GC invocation because we only read the maximal safe
  // timestamp once, and the version chain is sorted by timestamp. Here we keep a set of slots to truncate to avoid
  // wasteful traversals of the version chain.
  std::unordered_set<TupleSlot> visited_slots;

  // Process every transaction in the unlink queue
  while (!txns_to_unlink_.empty()) {
    txn = txns_to_unlink_.front();
    txns_to_unlink_.pop_front();

    if (txn->IsReadOnly()) {
      // This is a read-only transaction so this is safe to immediately delete
      delete txn;
      txns_processed++;
      readonly_processed++;
    } else if (transaction::TransactionUtil::NewerThan(oldest_txn, txn->FinishTime())) {
      // Safe to garbage collect.
      for (auto &undo_record : txn->undo_buffer_) {
        // It is possible for the table field to be null, for aborted transaction's last conflicting record
        DataTable *&table = undo_record.Table();
        // Each version chain needs to be traversed and truncated at most once every GC period. Check
        // if we have already visited this tuple slot; if not, proceed to prune the version chain.
        if (table != nullptr && visited_slots.insert(undo_record.Slot()).second)
          TruncateVersionChain(table, undo_record.Slot(), oldest_txn);
        // Regardless of the version chain we will need to reclaim deleted slots and any dangling pointers to varlens,
        // unless the transaction is aborted, and the record holds a version that is still visible.
        if (!txn->Aborted()) {
          ReclaimBufferIfVarlen(txn, &undo_record);
          ReclaimSlotIfDeleted(&undo_record);
        }
        if (observer_ != nullptr) observer_->ObserveWrite(undo_record.Slot().GetBlock());
        buffer_processed++;
      }
      txns_to_deallocate_.push_front(txn);
      txns_processed++;
    } else {
      // This is a committed txn that is still visible, requeue for next GC run
      requeue.push_front(txn);
    }
  }

  // Requeue any txns that we were still visible to running transactions
  txns_to_unlink_ = transaction::TransactionQueue(std::move(requeue));

  return std::make_tuple(txns_processed, buffer_processed, readonly_processed);
}

void GarbageCollector::ProcessDeferredActions(transaction::timestamp_t oldest_txn) {
  if (deferred_action_manager_ != DISABLED) {
    // TODO(Tianyu): Eventually we will remove the GC and implement version chain pruning with deferred actions
    deferred_action_manager_->Process(oldest_txn);
  }
}

void GarbageCollector::TruncateVersionChain(DataTable *const table, const TupleSlot slot,
                                            const transaction::timestamp_t oldest) const {
  const TupleAccessStrategy &accessor = table->accessor_;
  UndoRecord *const version_ptr = table->AtomicallyReadVersionPtr(slot, accessor);
  // This is a legitimate case where we truncated the version chain but had to restart because the previous head
  // was aborted.
  if (version_ptr == nullptr) return;

  // We need to special case the head of the version chain because contention with running transactions can happen
  // here. Instead of a blind update we will need to CAS and prune the entire version chain if the head of the version
  // chain can be GCed.
  if (transaction::TransactionUtil::NewerThan(oldest, version_ptr->Timestamp().load())) {
    if (!table->CompareAndSwapVersionPtr(slot, accessor, version_ptr, nullptr))
      // Keep retrying while there are conflicts, since we only invoke truncate once per GC period for every
      // version chain.
      TruncateVersionChain(table, slot, oldest);
    return;
  }

  // a version chain is guaranteed to not change when not at the head (assuming single-threaded GC), so we are safe
  // to traverse and update pointers without CAS
  UndoRecord *curr = version_ptr;
  UndoRecord *next;
  // Traverse until we find the earliest UndoRecord that can be unlinked.
  while (true) {
    next = curr->Next();
    // This is a legitimate case where we truncated the version chain but had to restart because the previous head
    // was aborted.
    if (next == nullptr) return;
    if (transaction::TransactionUtil::NewerThan(oldest, next->Timestamp().load())) break;
    curr = next;
  }
  // The rest of the version chain must also be invisible to any running transactions since our version
  // is newest-to-oldest sorted.
  curr->Next().store(nullptr);

  // If the head of the version chain was not committed, it could have been aborted and requires a retry.
  if (curr == version_ptr && !transaction::TransactionUtil::Committed(version_ptr->Timestamp().load()) &&
      table->AtomicallyReadVersionPtr(slot, accessor) != version_ptr)
    TruncateVersionChain(table, slot, oldest);
}

void GarbageCollector::ReclaimSlotIfDeleted(UndoRecord *const undo_record) const {
  if (undo_record->Type() == DeltaRecordType::DELETE) undo_record->Table()->accessor_.Deallocate(undo_record->Slot());
}

void GarbageCollector::ReclaimBufferIfVarlen(transaction::TransactionContext *const txn,
                                             UndoRecord *const undo_record) const {
  const TupleAccessStrategy &accessor = undo_record->Table()->accessor_;
  const BlockLayout &layout = accessor.GetBlockLayout();
  switch (undo_record->Type()) {
    case DeltaRecordType::INSERT:
      return;  // no possibility of outdated varlen to gc
    case DeltaRecordType::DELETE:
      // TODO(Tianyu): Potentially need to be more efficient than linear in column size?
      for (uint16_t i = 0; i < layout.NumColumns(); i++) {
        col_id_t col_id(i);
        // Okay to include version vector, as it is never varlen
        if (layout.IsVarlen(col_id)) {
          auto *varlen = reinterpret_cast<VarlenEntry *>(accessor.AccessWithNullCheck(undo_record->Slot(), col_id));
          if (varlen != nullptr && varlen->NeedReclaim()) txn->loose_ptrs_.push_back(varlen->Content());
        }
      }
      break;
    case DeltaRecordType::UPDATE:
      // TODO(Tianyu): This might be a really bad idea for large deltas...
      for (uint16_t i = 0; i < undo_record->Delta()->NumColumns(); i++) {
        col_id_t col_id = undo_record->Delta()->ColumnIds()[i];
        if (layout.IsVarlen(col_id)) {
          auto *varlen = reinterpret_cast<VarlenEntry *>(undo_record->Delta()->AccessWithNullCheck(i));
          if (varlen != nullptr && varlen->NeedReclaim()) txn->loose_ptrs_.push_back(varlen->Content());
        }
      }
      break;
    default:
      throw std::runtime_error("unexpected delta record type");
  }
}

void GarbageCollector::RegisterIndexForGC(const common::ManagedPointer<index::Index> index) {
  NOISEPAGE_ASSERT(index != nullptr, "Index cannot be nullptr.");
  common::SharedLatch::ScopedExclusiveLatch guard(&indexes_latch_);
  NOISEPAGE_ASSERT(indexes_.count(index) == 0, "Trying to register an index that has already been registered.");
  indexes_.insert(index);
}

void GarbageCollector::UnregisterIndexForGC(const common::ManagedPointer<index::Index> index) {
  NOISEPAGE_ASSERT(index != nullptr, "Index cannot be nullptr.");
  common::SharedLatch::ScopedExclusiveLatch guard(&indexes_latch_);
  NOISEPAGE_ASSERT(indexes_.count(index) == 1, "Trying to unregister an index that has not been registered.");
  indexes_.erase(index);
}

void GarbageCollector::ProcessIndexes() {
  common::SharedLatch::ScopedSharedLatch guard(&indexes_latch_);
  for (const auto &index : indexes_) index->PerformGarbageCollection();
}

}  // namespace noisepage::storage
