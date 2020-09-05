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

namespace terrier::storage {
class GarbageCollectorThread;
}
namespace terrier::transaction {

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
    TERRIER_ASSERT(new_deferred_actions_.empty(), "Some deferred actions remaining at time of destruction");
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
   * @param main_thread if this thread is in charge of process the index
   */
  void FullyPerformGC(const common::ManagedPointer <storage::GarbageCollector> gc,
                      const common::ManagedPointer <storage::LogManager> log_manager) {
    for (int i = 0; i < MIN_GC_INVOCATIONS; i++) {
      if (log_manager != DISABLED) log_manager->ForceFlush();
      // process deferred action queue as much as we can in each run
      gc->PerformGarbageCollection(false);
    }
  }

 private:
  friend class storage::GarbageCollectorThread;
  const common::ManagedPointer<TimestampManager> timestamp_manager_;
  // TODO(Tianyu): We might want to change this data structure to be more specialized than std::queue
  std::queue<std::pair<timestamp_t, std::pair<DeferredAction, DafId>>> new_deferred_actions_;
  std::atomic<uint32_t> queue_size_ = 0;
  common::SpinLatch queue_latch_;

  uint32_t ProcessNewActions(timestamp_t oldest_txn, bool metrics_enabled, bool with_limit);
  void ProcessNewActionHelper(timestamp_t oldest_txn, bool metrics_enabled, uint32_t *processed, bool *break_loop);
};
}  // namespace terrier::transaction
