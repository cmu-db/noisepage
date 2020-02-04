#include "storage/garbage_collector.h"

#include <unordered_set>
#include <utility>

#include "common/macros.h"
#include "loggers/storage_logger.h"
#include "storage/data_table.h"
#include "transaction/deferred_action_manager.h"
#include "transaction/transaction_context.h"
#include "transaction/transaction_defs.h"
#include "transaction/transaction_manager.h"
#include "transaction/transaction_util.h"

namespace terrier::storage {

// TODO(John:GC) Figure out how we keep the illusion of this for tests but ween the
// main system off this.  Initial thought is we can imitate this with static
// counters that are globally incremented in the callbacks (maybe tunable?).  It
// would be nice if this was hooked into metrics but that's a too far for right
// now.
std::pair<uint32_t, uint32_t> GarbageCollector::PerformGarbageCollection() {
  if (observer_ != nullptr) observer_->ObserveGCInvocation();
  timestamp_manager_->CheckOutTimestamp();
  const transaction::timestamp_t oldest_txn = timestamp_manager_->OldestTransactionStartTime();
  uint32_t txns_deallocated = ProcessDeallocateQueue(oldest_txn);
  STORAGE_LOG_TRACE("GarbageCollector::PerformGarbageCollection(): txns_deallocated: {}", txns_deallocated);
  uint32_t txns_unlinked = ProcessUnlinkQueue(oldest_txn);
  STORAGE_LOG_TRACE("GarbageCollector::PerformGarbageCollection(): txns_unlinked: {}", txns_unlinked);
  if (txns_unlinked > 0) {
    // Only update this field if we actually unlinked anything, otherwise we're being too conservative about when it's
    // safe to deallocate the transactions in our queue.
    last_unlinked_ = timestamp_manager_->CheckOutTimestamp();
  }
  STORAGE_LOG_TRACE("GarbageCollector::PerformGarbageCollection(): last_unlinked_: {}",
                    static_cast<uint64_t>(last_unlinked_));
  ProcessDeferredActions(oldest_txn);
  ProcessIndexes();

  // TODO(John:GC) We should be able to mimic this API interface with counters in the transaction manager that are
  // captured by reference and incremented during the unlink and deallocate actions for the transaction contexts and
  // then queried here.  That may require a resert mechanism and should look to eventually plug into Matt's metrics
  // system.
  return std::make_pair(txns_deallocated, txns_unlinked);
}

// TODO(John:GC) Refactor to be deferred inside unlinking below
uint32_t GarbageCollector::ProcessDeallocateQueue(transaction::timestamp_t oldest_txn) {
  uint32_t txns_processed = 0;
  bool gc_metrics_enabled =
      common::thread_context.metrics_store_ != nullptr &&
      common::thread_context.metrics_store_->ComponentToRecord(metrics::MetricsComponent::GARBAGECOLLECTION);

  if (gc_metrics_enabled) {
    // start the operating unit resource tracker
    common::thread_context.resource_tracker_.Start();
  }

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

  if (gc_metrics_enabled) {
    // Stop the resource tracker for this operating unit
    common::thread_context.resource_tracker_.Stop();
    if (txns_processed > 0) {
      auto &resource_metrics = common::thread_context.resource_tracker_.GetMetrics();
      common::thread_context.metrics_store_->RecordDeallocateData(txns_processed, resource_metrics);
    }
  }

  return txns_processed;
}

// TODO(John:GC) Move to TransactionContext class.  We defer this action at abort or commit to kick-off the GC logic.
uint32_t GarbageCollector::ProcessUnlinkQueue(transaction::timestamp_t oldest_txn) {
  transaction::TransactionContext *txn = nullptr;

  bool gc_metrics_enabled =
      common::thread_context.metrics_store_ != nullptr &&
      common::thread_context.metrics_store_->ComponentToRecord(metrics::MetricsComponent::GARBAGECOLLECTION);
  if (gc_metrics_enabled) {
    // start the operating unit resource tracker
    common::thread_context.resource_tracker_.Start();
  }
  uint64_t buffer_processed = 0, readonly_processed = 0;

  // Get the completed transactions from the TransactionManager
  transaction::TransactionQueue completed_txns = txn_manager_->CompletedTransactionsForGC();
  if (!completed_txns.empty()) {
    // Append to our local unlink queue
    txns_to_unlink_.splice_after(txns_to_unlink_.cbefore_begin(), std::move(completed_txns));
  }

  uint32_t txns_processed = 0;
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
          table->TruncateVersionChain(undo_record.Slot(), oldest_txn);
        // Regardless of the version chain we will need to reclaim deleted slots and any dangling pointers to varlens,
        // unless the transaction is aborted, and the record holds a version that is still visible.
        if (!txn->Aborted()) {
          undo_record.ReclaimSlotIfDeleted();
          undo_record.ReclaimBufferIfVarlen(txn);
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

  if (gc_metrics_enabled) {
    // Stop the resource tracker for this operating unit
    common::thread_context.resource_tracker_.Stop();
    if (txns_processed > 0) {
      auto &resource_metrics = common::thread_context.resource_tracker_.GetMetrics();
      common::thread_context.metrics_store_->RecordUnlinkData(txns_processed, buffer_processed, readonly_processed,
                                                              resource_metrics);
    }
  }

  return txns_processed;
}

void GarbageCollector::ProcessDeferredActions(transaction::timestamp_t oldest_txn) {
  if (deferred_action_manager_ != DISABLED) {
    // TODO(Tianyu): Eventually we will remove the GC and implement version chain pruning with deferred actions
    deferred_action_manager_->Process(oldest_txn);
  }
}

// TODO(John:GC) Where should this live?
void GarbageCollector::RegisterIndexForGC(const common::ManagedPointer<index::Index> index) {
  TERRIER_ASSERT(index != nullptr, "Index cannot be nullptr.");
  common::SharedLatch::ScopedExclusiveLatch guard(&indexes_latch_);
  TERRIER_ASSERT(indexes_.count(index) == 0, "Trying to register an index that has already been registered.");
  indexes_.insert(index);
}

// TODO(John:GC) Where should this live?
void GarbageCollector::UnregisterIndexForGC(const common::ManagedPointer<index::Index> index) {
  TERRIER_ASSERT(index != nullptr, "Index cannot be nullptr.");
  common::SharedLatch::ScopedExclusiveLatch guard(&indexes_latch_);
  TERRIER_ASSERT(indexes_.count(index) == 1, "Trying to unregister an index that has not been registered.");
  indexes_.erase(index);
}

// TODO(John:GC) What is the right way to invoke this?  Should it be a periodic task in the deferred action queue or
// special cased (which feels dirty)?  Frequency of call should not affect index performance just memory usage.  In
// fact, keeping transaction processing regulare should be more important for throughput than calling this.  Also, this
// potentially introduces a long pause in normal processing (could be bad) and could block (or be blocked by) DDL.
//
// @bug This should be exclusive because concurrent calls to PerformGarbageCollection for the same index may not be
// thread safe (i.e. bwtrees...)
void GarbageCollector::ProcessIndexes() {
  common::SharedLatch::ScopedSharedLatch guard(&indexes_latch_);
  for (const auto &index : indexes_) index->PerformGarbageCollection();
}

}  // namespace terrier::storage
