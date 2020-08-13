#include "transaction/deferred_action_manager.h"

#include "common/macros.h"

namespace terrier::transaction {

timestamp_t DeferredActionManager::RegisterDeferredAction(DeferredAction &&a, transaction::DafId daf_id) {
  timestamp_t result = timestamp_manager_->CurrentTime();
  std::pair<timestamp_t, std::pair<DeferredAction, DafId>> elem = {result, {a, daf_id}};

  // Timestamp needs to be fetched inside the critical section such that actions in the
  // deferred action queue is in order. This simplifies the interleavings we need to deal
  // with in the face of DDL changes.
  queue_latch_.Lock();
  new_deferred_actions_.push(elem);
  queue_latch_.Unlock();
  return result;
}

uint32_t DeferredActionManager::Process(bool process_index) {
  // TODO(John, Ling): this is now more conservative than it needs and can artificially delay garbage collection.
  //  We should be able to query the cached oldest transaction (should be cheap) in between each event
  //  and more aggressively clear the backlog abd the deferred event queue
  //  the point of taking oldest txn affect gc test.
  //  We could potentially more aggressively process the backlog and the deferred action queue
  //  by taking timestamp after processing each event
  timestamp_manager_->CheckOutTimestamp();
  auto begin = timestamp_manager_->BeginTransaction();
  const transaction::timestamp_t oldest_txn = timestamp_manager_->OldestTransactionStartTime();
  // Check out a timestamp from the transaction manager to determine the progress of
  // running transactions in the system.

  bool daf_metrics_enabled =
      common::thread_context.metrics_store_ != nullptr &&
      common::thread_context.metrics_store_->ComponentToRecord(metrics::MetricsComponent::GARBAGECOLLECTION);

  uint32_t processed = ProcessNewActions(oldest_txn, daf_metrics_enabled);

  if (process_index) ProcessIndexes();
  common::thread_context.visited_slots_.clear();
  timestamp_manager_->RemoveTransaction(begin);
//
//  if (daf_metrics_enabled) {
//    common::thread_context.metrics_store_->RecordAfterQueueSize(back_log_.size() + new_deferred_actions_.unsafe_size());
//  }
  return processed;
}

void DeferredActionManager::RegisterIndexForGC(const common::ManagedPointer<storage::index::Index> index) {
  TERRIER_ASSERT(index != nullptr, "Index cannot be nullptr.");
  common::SharedLatch::ScopedExclusiveLatch guard(&indexes_latch_);
  TERRIER_ASSERT(indexes_.count(index) == 0, "Trying to register an index that has already been registered.");
  indexes_.insert(index);
}

void DeferredActionManager::UnregisterIndexForGC(const common::ManagedPointer<storage::index::Index> index) {
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
void DeferredActionManager::ProcessIndexes() {
  common::SharedLatch::ScopedSharedLatch guard(&indexes_latch_);
  for (const auto &index : indexes_) index->PerformGarbageCollection();
}

uint32_t DeferredActionManager::ProcessNewActions(timestamp_t oldest_txn, bool metrics_enabled) {
  uint32_t processed = 0;
  while (true) {
    queue_latch_.Lock();
    if (processed == 0 && metrics_enabled) common::thread_context.metrics_store_->RecordQueueSize(new_deferred_actions_.size());

    if (new_deferred_actions_.empty() || !transaction::TransactionUtil::NewerThan(oldest_txn, new_deferred_actions_.front().first)) {
      queue_latch_.Unlock();
      break;
    }

    std::pair<timestamp_t, std::pair<DeferredAction, DafId>> curr_action = new_deferred_actions_.front();
    new_deferred_actions_.pop();
    queue_latch_.Unlock();
    if (metrics_enabled) common::thread_context.resource_tracker_.Start();

    curr_action.second.first(oldest_txn);

    if (metrics_enabled) {
      common::thread_context.resource_tracker_.Stop();
      auto &resource_metrics = common::thread_context.resource_tracker_.GetMetrics();
      common::thread_context.metrics_store_->RecordActionData(curr_action.second.second, resource_metrics);
    }
    processed++;

  }
  return processed;
}

}  // namespace terrier::transaction
