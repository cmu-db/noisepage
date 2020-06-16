#include "transaction/deferred_action_manager.h"

#include "common/macros.h"

namespace terrier::transaction {

timestamp_t DeferredActionManager::RegisterDeferredAction(DeferredAction &&a, transaction::DafId daf_id) {
  timestamp_t result = timestamp_manager_->CurrentTime();
  std::pair<timestamp_t, std::pair<DeferredAction, DafId>> elem = {result, {a, daf_id}};

  // Timestamp needs to be fetched inside the critical section such that actions in the
  // deferred action queue is in order. This simplifies the interleavings we need to deal
  // with in the face of DDL changes.
  new_deferred_actions_.push(elem);
  return result;
}

uint32_t DeferredActionManager::Process() {
  // TODO(John, Ling): this is now more conservative than it needs and can artificially delay garbage collection.
  //  We should be able to query the cached oldest transaction (should be cheap) in between each event
  //  and more aggressively clear the backlog abd the deferred event queue
  //  the point of taking oldest txn affect gc test.
  //  We could potentially more aggressively process the backlog and the deferred action queue
  //  by taking timestamp after processing each event
  timestamp_manager_->CheckOutTimestamp();
  const transaction::timestamp_t oldest_txn = timestamp_manager_->OldestTransactionStartTime();
  // Check out a timestamp from the transaction manager to determine the progress of
  // running transactions in the system.
  const auto backlog_size = static_cast<uint32_t>(back_log_.size());
  bool daf_metrics_enabled =
      common::thread_context.metrics_store_ != nullptr &&
      common::thread_context.metrics_store_->ComponentToRecord(metrics::MetricsComponent::GARBAGECOLLECTION);
  if (daf_metrics_enabled) {
    common::thread_context.metrics_store_->RecordDafWakeup();
  }

  uint32_t processed = ClearBacklog(oldest_txn, daf_metrics_enabled);
  // There is no point in draining new actions if we haven't cleared the backlog.
  // This leaves some mechanisms for the rest of the system to detect congestion
  // at the deferred action manager and potentially backoff
  if (backlog_size == processed) {
    // ingest all the new actions
    processed += ProcessNewActions(oldest_txn, daf_metrics_enabled);
  }
  ProcessIndexes();
  visited_slots_.clear();
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
uint32_t DeferredActionManager::ClearBacklog(timestamp_t oldest_txn, bool metrics_enabled) {
  uint32_t processed = 0;
  // Execute as many deferred actions as we can at this time from the backlog.
  // TODO(Tianyu): This will not work if somehow the timestamps we compare against has sign bit flipped.
  //  (for uncommitted transactions, or on overflow)
  // Although that should never happen, we need to be aware that this might be a problem in the future.
  if (metrics_enabled) common::thread_context.metrics_store_->RecordQueueSize(back_log_.size());
//  std::cout << "backlog size" << back_log_.size() << std::endl;
  while (!back_log_.empty() && transaction::TransactionUtil::NewerThan(oldest_txn, back_log_.front().first)) {
    if (metrics_enabled) common::thread_context.resource_tracker_.Start();
    back_log_.front().second.first(oldest_txn);
    if (metrics_enabled) {
      common::thread_context.resource_tracker_.Stop();
      auto &resource_metrics = common::thread_context.resource_tracker_.GetMetrics();
      common::thread_context.metrics_store_->RecordActionData(back_log_.front().second.second, resource_metrics);
//      common::thread_context.metrics_store_->RecordQueueSize(1);
    }
    processed++;
//    if (metrics_enabled) common::thread_context.metrics_store_->RecordQueueSize(1);
    back_log_.pop();
  }
  return processed;
}

uint32_t DeferredActionManager::ProcessNewActions(timestamp_t oldest_txn, bool metrics_enabled) {
  uint32_t processed = 0;
  std::pair<timestamp_t, std::pair<DeferredAction, DafId>> curr_action = {
      timestamp_t(0), {[=](timestamp_t /*unused*/) {}, DafId::INVALID}};
  auto curr_size = new_deferred_actions_.unsafe_size();
  if (metrics_enabled) {
    common::thread_context.metrics_store_->RecordQueueSize(back_log_.size() + curr_size);
  }
  while (processed != curr_size) {
    // Try_pop would pop the front of the queue if there is at least element in queue,
    // Since currently the deferred action queue only has one consumer, if the while loop condition is satifsfied
    // try pop should always return true
    bool has_item UNUSED_ATTRIBUTE = new_deferred_actions_.try_pop(curr_action);
    TERRIER_ASSERT(has_item,
                   "With single consumer of queue, we should be able to pop front when we have not processed every "
                   "item in the queue.");
    if (!transaction::TransactionUtil::NewerThan(oldest_txn, curr_action.first)) break;
    if (metrics_enabled) common::thread_context.resource_tracker_.Start();

    curr_action.second.first(oldest_txn);

    if (metrics_enabled) {
      common::thread_context.resource_tracker_.Stop();
      auto &resource_metrics = common::thread_context.resource_tracker_.GetMetrics();
      common::thread_context.metrics_store_->RecordActionData(curr_action.second.second, resource_metrics);
    }
    processed++;
  }
  if (processed != curr_size && curr_action.first != INVALID_TXN_TIMESTAMP) back_log_.push(curr_action);
  if (metrics_enabled) {
//    if (!common::thread_context.metrics_store_->CheckWakeUp())
////      std::cout << "dfdfd" << std::endl;
    common::thread_context.metrics_store_->RecordAfterQueueSize(back_log_.size() + curr_size - processed);
  }
  return processed;
}

}  // namespace terrier::transaction
