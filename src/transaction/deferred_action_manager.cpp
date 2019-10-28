#include "transaction/deferred_action_manager.h"
#include <unordered_set>
#include <utility>
#include "common/macros.h"
#include "loggers/storage_logger.h"
#include "storage/data_table.h"
#include "storage/garbage_collector.h"
#include "transaction/deferred_action_manager.h"
#include "transaction/transaction_context.h"
#include "transaction/transaction_defs.h"
#include "transaction/transaction_util.h"

namespace terrier::transaction {

timestamp_t DeferredActionManager::RegisterDeferredAction(const DeferredAction &a) {
  common::SpinLatch::ScopedSpinLatch guard(&deferred_actions_latch_);
  // Timestamp needs to be fetched inside the critical section such that actions in the
  // deferred action queue is in order. This simplifies the interleavings we need to deal
  // with in the face of DDL changes.
  timestamp_t result = timestamp_manager_->CurrentTime();
  new_deferred_actions_.emplace(result, a);
  return result;
}

timestamp_t DeferredActionManager::RegisterDeferredAction(const std::function<void()> &a) {
  // TODO(Tianyu): Will this be a performance problem? Hopefully C++ is smart enough
  // to optimize out this call...
  return RegisterDeferredAction([=](timestamp_t /*unused*/) { a(); });
}

uint32_t DeferredActionManager::Process() {
  // Check out a timestamp from the transaction manager to determine the progress of
  // running transactions in the system.
  const transaction::timestamp_t oldest_txn = timestamp_manager_->OldestTransactionStartTime();
  const auto backlog_size = static_cast<uint32_t>(back_log_.size());
  uint32_t processed = ClearBacklog(oldest_txn);
  // There is no point in draining new actions if we haven't cleared the backlog.
  // This leaves some mechanisms for the rest of the system to detect congestion
  // at the deferred action manager and potentially backoff
  if (backlog_size != processed) return processed;
  // Otherwise, ingest all the new actions
  processed += ProcessNewActions(oldest_txn);

  // TODO(yash) This should go to a dedicated thread or be done away with when BW-Tree is removed
  ProcessIndexes();
  return processed;
}

void DeferredActionManager::RegisterIndexForGC(common::ManagedPointer<storage::index::Index> index) {
  TERRIER_ASSERT(index != nullptr, "Index cannot be nullptr.");
  common::SharedLatch::ScopedExclusiveLatch guard(&indexes_latch_);
  TERRIER_ASSERT(indexes_.count(index) == 0, "Trying to register an index that has already been registered.");
  indexes_.insert(index);
}

void DeferredActionManager::UnregisterIndexForGC(common::ManagedPointer<storage::index::Index> index) {
  TERRIER_ASSERT(index != nullptr, "Index cannot be nullptr.");
  common::SharedLatch::ScopedExclusiveLatch guard(&indexes_latch_);
  TERRIER_ASSERT(indexes_.count(index) == 1, "Trying to unregister an index that has not been registered.");
  indexes_.erase(index);
}

uint32_t DeferredActionManager::ClearBacklog(timestamp_t oldest_txn) {
  uint32_t processed = 0;
  // Execute as many deferred actions as we can at this time from the backlog.
  // Stop traversing
  // TODO(Tianyu): This will not work if somehow the timestamps we compare against has sign bit flipped.
  //  (for uncommiitted transactions, or on overflow)
  // Although that should never happen, we need to be aware that this might be a problem in the future.
  while (!back_log_.empty() && oldest_txn >= back_log_.front().first) {
    back_log_.front().second(oldest_txn);
    processed++;
    back_log_.pop();
  }
  return processed;
}

uint32_t DeferredActionManager::ProcessNewActions(timestamp_t oldest_txn) {
  uint32_t processed = 0;
  // swap the new actions queue with a local queue, so the rest of the system can continue
  // while we process actions
  std::queue<std::pair<timestamp_t, DeferredAction>> new_actions_local;
  {
    common::SpinLatch::ScopedSpinLatch guard(&deferred_actions_latch_);
    new_actions_local = std::move(new_deferred_actions_);
  }

  // Iterate through the new actions queue and execute as many as possible
  while (!new_actions_local.empty() && oldest_txn >= new_actions_local.front().first) {
    new_actions_local.front().second(oldest_txn);
    processed++;
    new_actions_local.pop();
  }

  // Add the rest to back log otherwise
  while (!new_actions_local.empty()) {
    back_log_.push(new_actions_local.front());
    new_actions_local.pop();
  }
  return processed;
}

void DeferredActionManager::ProcessIndexes() {
  common::SharedLatch::ScopedSharedLatch guard(&indexes_latch_);
  for (const auto &index : indexes_) index->PerformGarbageCollection();
}
}  // namespace terrier::transaction