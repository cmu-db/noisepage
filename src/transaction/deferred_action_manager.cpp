#include "transaction/deferred_action_manager.h"

#include "common/macros.h"
#include "storage/index/index.h"

namespace noisepage::transaction {

timestamp_t DeferredActionManager::RegisterDeferredAction(DeferredAction &&a, transaction::DafId daf_id) {
  timestamp_t result = timestamp_manager_->CurrentTime();
  std::pair<timestamp_t, std::pair<DeferredAction, DafId>> elem = {result, {a, daf_id}};

  // Timestamp needs to be fetched inside the critical section such that actions in the
  // deferred action queue is in order. This simplifies the interleavings we need to deal
  // with in the face of DDL changes.
  queue_latch_.Lock();
  new_deferred_actions_.push(elem);
  queue_latch_.Unlock();
  queue_size_++;
  return result;
}

uint32_t DeferredActionManager::Process(bool with_limit) {
  bool daf_metrics_enabled =
      common::thread_context.metrics_store_ != nullptr &&
      common::thread_context.metrics_store_->ComponentToRecord(metrics::MetricsComponent::GARBAGECOLLECTION);

  if (daf_metrics_enabled) common::thread_context.metrics_store_->RecordQueueSize(queue_size_);
  // TODO(John, Ling): this is now more conservative than it needs and can artificially delay garbage collection.
  //  We should be able to query the cached oldest transaction (should be cheap) in between each event
  //  and more aggressively clear the deferred event queue.
  //  The point of taking oldest txn affect gc test.
  timestamp_manager_->CheckOutTimestamp();
  auto begin = timestamp_manager_->BeginTransaction();
  const transaction::timestamp_t oldest_txn = timestamp_manager_->OldestTransactionStartTime();
  // Check out a timestamp from the transaction manager to determine the progress of
  // running transactions in the system.

  uint32_t processed = ProcessNewActions(oldest_txn, daf_metrics_enabled, with_limit);
  timestamp_manager_->RemoveTransaction(begin);

  ProcessIndexes();
  auto previous_size = common::thread_context.visited_slots_.size();
  common::thread_context.visited_slots_.clear();
  common::thread_context.visited_slots_.reserve(previous_size);
  return processed;
}

void DeferredActionManager::RegisterIndexForGC(const common::ManagedPointer<storage::index::Index> index) {
  NOISEPAGE_ASSERT(index != nullptr, "Index cannot be nullptr.");
  common::SharedLatch::ScopedExclusiveLatch guard(&indexes_latch_);
  NOISEPAGE_ASSERT(indexes_.count(index) == 0, "Trying to register an index that has already been registered.");
  indexes_.insert(index);
}

void DeferredActionManager::UnregisterIndexForGC(const common::ManagedPointer<storage::index::Index> index) {
  NOISEPAGE_ASSERT(index != nullptr, "Index cannot be nullptr.");
  common::SharedLatch::ScopedExclusiveLatch guard(&indexes_latch_);
  NOISEPAGE_ASSERT(indexes_.count(index) == 1, "Trying to unregister an index that has not been registered.");
  indexes_.erase(index);
}

// TODO(John:GC) What is the right way to invoke this?  Should it be a periodic task in the deferred action queue or
// special cased (which feels dirty)?  Frequency of call should not affect index performance just memory usage.  In
// fact, keeping transaction processing regular should be more important for throughput than calling this.  Also, this
// potentially introduces a long pause in normal processing (could be bad) and could block (or be blocked by) DDL.
//
void DeferredActionManager::ProcessIndexes() {
  if (indexes_latch_.TryExclusiveLock()) {
    for (const auto &index : indexes_) index->PerformGarbageCollection();
    indexes_latch_.UnlockExclusive();
  }
}

uint32_t DeferredActionManager::ProcessNewActions(timestamp_t oldest_txn, bool metrics_enabled, bool with_limit) {
  uint32_t processed = 0;
  std::queue<std::pair<timestamp_t, std::pair<DeferredAction, DafId>>> temp_action_queue;
  bool break_loop = false;

  if (with_limit) {
    // During normal execution, bound the size of number of actions processed per run
    // so that cleaning the deferred actions does not become the long-running transaction
    for (size_t iter = 0; iter < MAX_ACTION_PER_RUN; iter++) {
      ProcessNewActionHelper(oldest_txn, metrics_enabled, &processed, &break_loop);
      if (break_loop) break;
    }
  } else {
    // When clearing up the deferred action queue at the end of execution process all actions
    // that can be processed at each run to ensuring process all actions in the queue
    while (true) {
      ProcessNewActionHelper(oldest_txn, metrics_enabled, &processed, &break_loop);
      if (break_loop) break;
    }
  }

  return processed;
}

void DeferredActionManager::ProcessNewActionHelper(timestamp_t oldest_txn, bool metrics_enabled, uint32_t *processed,
                                                   bool *break_loop) {
  std::queue<std::pair<timestamp_t, std::pair<DeferredAction, DafId>>> temp_action_queue;
  // pop a batch of actions
  queue_latch_.Lock();
  for (size_t i = 0; i < BATCH_SIZE; i++) {
    if (new_deferred_actions_.empty() ||
        !transaction::TransactionUtil::NewerThan(oldest_txn, new_deferred_actions_.front().first)) {
      *break_loop = true;
      break;
    }

    std::pair<timestamp_t, std::pair<DeferredAction, DafId>> curr_action = new_deferred_actions_.front();
    new_deferred_actions_.pop();
    temp_action_queue.push(curr_action);
    queue_size_--;
  }
  queue_latch_.Unlock();

  // process a batch of actions
  while (!temp_action_queue.empty()) {
    temp_action_queue.front().second.first(oldest_txn);

    if (metrics_enabled) {
      common::thread_context.metrics_store_->RecordActionData(temp_action_queue.front().second.second);
    }
    (*processed)++;
    temp_action_queue.pop();
  }
}

}  // namespace noisepage::transaction
