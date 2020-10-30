#pragma once
#include <queue>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/thread_context.h"
#include "metrics/metrics_store.h"
#include "storage/garbage_collector.h"
#include "storage/write_ahead_log/log_manager.h"
#include "transaction/timestamp_manager.h"
#include "transaction/transaction_defs.h"

namespace noisepage::storage {
class GarbageCollectorThread;
}
namespace noisepage::transaction {

constexpr uint8_t BATCH_SIZE = 6;

/**
 * The deferred action manager tracks deferred actions and provides a function to process them
 */
class DeferredActionManager {
 public:
  /**
   * Constructs a new DeferredActionManager
   * @param timestamp_manager source of timestamps in the system. If the timestamp manager is nullptr, it means GC is
   * off.
   */
  explicit DeferredActionManager(const common::ManagedPointer<TimestampManager> timestamp_manager)
      : timestamp_manager_(timestamp_manager) {}

  ~DeferredActionManager() {
    common::SpinLatch::ScopedSpinLatch guard(&queue_latch_);
    NOISEPAGE_ASSERT(new_deferred_actions_.empty(), "Some deferred actions remaining at time of destruction");
  }

  /**
   * Adds the action to a buffered list of deferred actions.  This action will
   * be triggered no sooner than when the epoch (timestamp of oldest running
   * transaction) is more recent than the time this function was called.
   * @param a functional implementation of the action that is deferred. @see DeferredAction
   * @param daf_id id of the type of this deferred action
   */
  timestamp_t RegisterDeferredAction(DeferredAction &&a, transaction::DafId daf_id);

  /**
   * Adds the action to a buffered list of deferred actions.  This action will
   * be triggered no sooner than when the epoch (timestamp of oldest running
   * transaction) is more recent than the time this function was called.
   * @param a functional implementation of the action that is deferred
   * @param daf_id id of the type of this deferred action
   */
  timestamp_t RegisterDeferredAction(const std::function<void()> &a, DafId daf_id) {
    // TODO(Tianyu): Will this be a performance problem? Hopefully C++ is smart enough
    // to optimize out this call...
    return RegisterDeferredAction([=](timestamp_t /*unused*/) { a(); }, daf_id);
  }

  /**
   * Clear the queue and apply as many actions as possible. Used in multi-threaded DAF.
   * @param with_limit If there is an upper bound on number of actions processed in each invocation
   * @return numbers of deferred actions processed
   */
  uint32_t Process(bool with_limit);

  /**
   * Invokes GC and log manager enough times to fully GC any outstanding transactions and process deferred events.
   * Currently, this must be done 3 times. The log manager must be called because transactions can only be GC'd once
   * their logs are persisted.
   *
   * @param gc gc to use for garbage collection
   * @param log_manager log manager to use for flushing logs
   */
  void FullyPerformGC(const common::ManagedPointer<storage::GarbageCollector> gc,
                      const common::ManagedPointer<storage::LogManager> log_manager) {
    for (int i = 0; i < MIN_GC_INVOCATIONS; i++) {
      if (log_manager != DISABLED) log_manager->ForceFlush();
      // process deferred action queue as much as we can in each run
      gc->PerformGarbageCollection(false);
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
  friend class storage::GarbageCollectorThread;
  const common::ManagedPointer<TimestampManager> timestamp_manager_;
  // TODO(Tianyu): We might want to change this data structure to be more specialized than std::queue
  std::queue<std::pair<timestamp_t, std::pair<DeferredAction, DafId>>> new_deferred_actions_;

  std::atomic<uint32_t> queue_size_ = 0;

  std::unordered_set<common::ManagedPointer<storage::index::Index>> indexes_;
  common::SharedLatch indexes_latch_;
  common::SpinLatch queue_latch_;

  // TODO(John, Ling): Eventually we should remove the special casing of indexes here.
  //  This gets invoked every epoch to look through all indexes. It potentially introduces stalls
  //  and looks inefficient if there is not much to gc. Preferably make index gc action a deferred action that gets
  //  added to the deferred action queue either in a fixed interval or after a threshold number of tombstones
  //  However, we can't simply remove the vector of index until we make the PerformGC method of Bwtree concurrent
  void ProcessIndexes();

  uint32_t ProcessNewActions(timestamp_t oldest_txn, bool metrics_enabled, bool with_limit);
  void ProcessNewActionHelper(timestamp_t oldest_txn, bool metrics_enabled, uint32_t *processed, bool *break_loop);
};
}  // namespace noisepage::transaction
