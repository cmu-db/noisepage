#pragma once
#include <queue>
#include <unordered_set>
#include <utility>
#include <vector>
#include <tbb/concurrent_queue.h>

#include "storage/garbage_collector.h"
#include "storage/write_ahead_log/log_manager.h"
#include "transaction/timestamp_manager.h"
#include "transaction/transaction_defs.h"

namespace terrier::transaction {

constexpr uint8_t MIN_GC_INVOCATIONS = 3;

/**
 * The deferred action manager tracks deferred actions and provides a function to process them
 */
class DeferredActionManager {
 public:
  /**
   * Constructs a new DeferredActionManager
   * @param timestamp_manager source of timestamps in the system
   */
  explicit DeferredActionManager(const common::ManagedPointer<TimestampManager> timestamp_manager)
      : timestamp_manager_(timestamp_manager) {}

  ~DeferredActionManager() {
    common::SharedLatch::ScopedExclusiveLatch guard(&deferred_actions_latch_);
    TERRIER_ASSERT(back_log_.empty(), "Backlog is not empty");
    TERRIER_ASSERT(new_deferred_actions_.empty(), "Some deferred actions remaining at time of destruction");
  }

  /**
   * Adds the action to a buffered list of deferred actions.  This action will
   * be triggered no sooner than when the epoch (timestamp of oldest running
   * transaction) is more recent than the time this function was called.
   * @param a functional implementation of the action that is deferred. @see DeferredAction
   */
  timestamp_t RegisterDeferredAction(DeferredAction &&a) {
    timestamp_t result = timestamp_manager_->CurrentTime();
    std::pair<timestamp_t, DeferredAction> elem = {result, a};

    common::SharedLatch::ScopedSharedLatch guard(&deferred_actions_latch_);
    // Timestamp needs to be fetched inside the critical section such that actions in the
    // deferred action queue is in order. This simplifies the interleavings we need to deal
    // with in the face of DDL changes.
    new_deferred_actions_.push(std::move(elem));
    return result;
  }

  /**
   * Adds the action to a buffered list of deferred actions.  This action will
   * be triggered no sooner than when the epoch (timestamp of oldest running
   * transaction) is more recent than the time this function was called.
   * @param a functional implementation of the action that is deferred
   */
  timestamp_t RegisterDeferredAction(const std::function<void()> &a) {
    // TODO(Tianyu): Will this be a performance problem? Hopefully C++ is smart enough
    // to optimize out this call...
    return RegisterDeferredAction([=](timestamp_t /*unused*/) { a(); });
  }

  /**
   * Clear the queue and apply as many actions as possible
   * @return numbers of deferred actions processed
   */
  uint32_t Process() {
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
    uint32_t processed = ClearBacklog(oldest_txn);
    // There is no point in draining new actions if we haven't cleared the backlog.
    // This leaves some mechanisms for the rest of the system to detect congestion
    // at the deferred action manager and potentially backoff
    if (backlog_size == processed) {
      // ingest all the new actions
      processed += ProcessNewActions(oldest_txn);
    }
    ProcessIndexes();
    return processed;
  }

  /**
   * Invokes GC and log manager enough times to fully GC any outstanding transactions and process deferred events.
   * Currently, this must be done 3 times. The log manager must be called because transactions can only be GC'd once
   * their logs are persisted.
   * @param gc gc to use for garbage collection
   * @param log_manager log manager to use for flushing logs
   */
  void FullyPerformGC(const common::ManagedPointer<storage::GarbageCollector> gc,
                      const common::ManagedPointer<storage::LogManager> log_manager) {
    for (int i = 0; i < MIN_GC_INVOCATIONS; i++) {
      if (log_manager != DISABLED) log_manager->ForceFlush();
      gc->PerformGarbageCollection();
    }
  }

  /**
   * // TODO(John, Ling): Eventually we should remove the special casing of indexes here. See processIndexes()
   * Register an index to be periodically garbage collected
   * @param index pointer to the index to register
   */
  void RegisterIndexForGC(common::ManagedPointer<storage::index::Index> index);

  /**
   * // TODO(John, Ling): Eventually we should remove the special casing of indexes here. See processIndexes()
   * Unregister an index to be periodically garbage collected
   * @param index pointer to the index to unregister
   */
  void UnregisterIndexForGC(common::ManagedPointer<storage::index::Index> index);

 private:
  const common::ManagedPointer<TimestampManager> timestamp_manager_;
  // TODO(Tianyu): We might want to change this data structure to be more specialized than std::queue
  tbb::concurrent_queue<std::pair<timestamp_t, DeferredAction>> new_deferred_actions_;
  std::queue<std::pair<timestamp_t, DeferredAction>> back_log_;
  common::SharedLatch deferred_actions_latch_;

  std::unordered_set<common::ManagedPointer<storage::index::Index>> indexes_;
  common::SharedLatch indexes_latch_;

  // TODO(John, Ling): Eventually we should remove the special casing of indexes here.
  //  This gets invoked every epoch to look through all indexes. It potentially introduces stalls
  //  and looks inefficient if there is not much to gc. Preferably make index gc action a deferred action that gets
  //  added to the deferred action queue either in a fixed interval or after a threshold number of tombstones
  void ProcessIndexes();

  uint32_t ClearBacklog(timestamp_t oldest_txn) {
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

  uint32_t ProcessNewActions(timestamp_t oldest_txn) {
    uint32_t processed = 0;
    // swap the new actions queue with a local queue, so the rest of the system can continue
    // while we process actions
//    tbb::concurrent_queue<std::pair<timestamp_t, DeferredAction>> new_actions_local;
//    {
//      common::SpinLatch::ScopedSpinLatch guard(&deferred_actions_latch_);
//      new_actions_local = std::move(new_deferred_actions_);
//    }

    deferred_actions_latch_.LockExclusive();
    tbb::concurrent_queue<std::pair<timestamp_t, DeferredAction>> new_actions_local(std::move(new_deferred_actions_));
    deferred_actions_latch_.Unlock();

    std::pair<timestamp_t, DeferredAction> curr_action;
    bool reinsert = false;
    while (!new_actions_local.empty()) {
      reinsert = new_actions_local.try_pop(curr_action);
      if (reinsert && oldest_txn < curr_action.first) break;

      curr_action.second(oldest_txn);
      processed++;
      reinsert = false;
    }
    if (reinsert) back_log_.push(curr_action);
//
//    // Iterate through the new actions queue and execute as many as possible
//    while (!new_actions_local.empty() && oldest_txn >= new_actions_local.front().first) {
//      new_actions_local.front().second(oldest_txn);
//      processed++;
//      new_actions_local.pop();
//    }

    // Add the rest to back log otherwise
    while (!new_actions_local.empty()) {
      new_actions_local.try_pop(curr_action);
      back_log_.push(curr_action);
    }
    return processed;
  }
};
}  // namespace terrier::transaction
