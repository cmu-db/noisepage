#pragma once
#include <tbb/concurrent_queue.h>
#include <queue>
#include <unordered_set>
#include <utility>
#include <vector>

#include "storage/garbage_collector.h"
#include "storage/write_ahead_log/log_manager.h"
#include "transaction/timestamp_manager.h"
#include "transaction/transaction_defs.h"

namespace terrier::storage {
class GarbageCollectorThread;
}
namespace terrier::transaction {

constexpr uint8_t MIN_GC_INVOCATIONS = 3;
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
      : timestamp_manager_(timestamp_manager), daf_tags_(DAF_TAG_COUNT) {
    for (size_t i = 0; i < DAF_TAG_COUNT; i++)  // NOLINT
      daf_tags_[i] = 0;
  }

  ~DeferredActionManager() {
//    TERRIER_ASSERT(back_log_.empty(), "Backlog is not empty");
    TERRIER_ASSERT(new_deferred_actions_.empty(), "Some deferred actions remaining at time of destruction");
  }

  /**
   * Adds the action to a buffered list of deferred actions.  This action will
   * be triggered no sooner than when the epoch (timestamp of oldest running
   * transaction) is more recent than the time this function was called.
   * @param a functional implementation of the action that is deferred. @see DeferredAction
   */
  timestamp_t RegisterDeferredAction(DeferredAction &&a, transaction::DafId daf_id);

  /**
   * Adds the action to a buffered list of deferred actions.  This action will
   * be triggered no sooner than when the epoch (timestamp of oldest running
   * transaction) is more recent than the time this function was called.
   * @param a functional implementation of the action that is deferred
   */
  timestamp_t RegisterDeferredAction(const std::function<void()> &a, DafId daf_id) {
    // TODO(Tianyu): Will this be a performance problem? Hopefully C++ is smart enough
    // to optimize out this call...
    return RegisterDeferredAction([=](timestamp_t /*unused*/) { a(); }, daf_id);
  }

  /**
   * Clear the queue and apply as many actions as possible. Used in single-threaded GC.
   * @return numbers of deferred actions processed
   */
  uint32_t Process();

  /**
   * Clear the queue and apply as many actions as possible. Used in multi-threaded GC.
   * @return numbers of deferred actions processed
   */
  uint32_t Process(bool process_index);

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
  friend class storage::GarbageCollectorThread;
  const common::ManagedPointer<TimestampManager> timestamp_manager_;
  // TODO(Tianyu): We might want to change this data structure to be more specialized than std::queue
  tbb::concurrent_queue<std::pair<timestamp_t, std::pair<DeferredAction, DafId>>> new_deferred_actions_;
  std::queue<std::pair<timestamp_t, std::pair<DeferredAction, DafId>>> back_log_;
  // It is sufficient to truncate each version chain once in a GC invocation because we only read the maximal safe
  // timestamp once, and the version chain is sorted by timestamp. Here we keep a set of slots to truncate to avoid
  // wasteful traversals of the version chain.
  std::unordered_set<storage::TupleSlot> visited_slots_;

  std::unordered_set<common::ManagedPointer<storage::index::Index>> indexes_;
  common::SharedLatch indexes_latch_;
  std::vector<std::atomic<uint16_t>> daf_tags_;
  std::atomic<size_t> back_log_count_ = 0;

  // TODO(John, Ling): Eventually we should remove the special casing of indexes here.
  //  This gets invoked every epoch to look through all indexes. It potentially introduces stalls
  //  and looks inefficient if there is not much to gc. Preferably make index gc action a deferred action that gets
  //  added to the deferred action queue either in a fixed interval or after a threshold number of tombstones
  void ProcessIndexes();

  uint32_t ClearBacklog(timestamp_t oldest_txn, bool metrics_enabled);

  uint32_t ProcessNewActions(timestamp_t oldest_txn, bool metrics_enabled);
};
}  // namespace terrier::transaction
