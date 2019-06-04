#pragma once

#include <chrono>  //NOLINT
#include <thread>  //NOLINT
#include <unordered_set>
#include "common/shared_latch.h"
#include "storage/garbage_collector.h"
#include "storage/index/index.h"

namespace terrier::storage {

/**
 * Class for spinning off a thread that runs garbage collection at a fixed interval. This should be used in most cases
 * to enable GC in the system unless you need fine-grained control over table state or profiling.
 */
class GarbageCollectorThread {
 public:
  /**
   * @param txn_manager pointer to the txn manager for the GC to communicate with
   * @param gc_period sleep time between GC invocations
   */
  GarbageCollectorThread(transaction::TransactionManager *const txn_manager, const std::chrono::milliseconds gc_period)
      : run_gc_(true),
        gc_paused_(false),
        gc_(txn_manager),
        gc_period_(gc_period),
        gc_thread_(std::thread([this] { GCThreadLoop(); })) {}

  ~GarbageCollectorThread() {
    run_gc_ = false;
    gc_thread_.join();
    // Make sure all garbage is collected. This take 2 runs for unlink and deallocate
    // TODO(Matt): these semantics may change as the GC becomes a more general deferred event framework
    gc_.PerformGarbageCollection();
    gc_.PerformGarbageCollection();
  }

  /**
   * Pause the GC from running, typically for use in tests when the state of tables need to be fixed.
   */
  void PauseGC() {
    TERRIER_ASSERT(!gc_paused_, "GC should not already be paused.");
    gc_paused_ = true;
  }

  /**
   * Resume GC after being paused.
   */
  void ResumeGC() {
    TERRIER_ASSERT(gc_paused_, "GC should already be paused.");
    gc_paused_ = false;
  }

  /**
   * Register an index to be periodically garbage collected
   * @param index pointer to the index to register
   */
  void RegisterIndexForGC(index::Index *const index) {
    TERRIER_ASSERT(index != nullptr, "Index cannot be nullptr.");
    common::SharedLatch::ScopedExclusiveLatch guard(&indexes_latch_);
    TERRIER_ASSERT(indexes_.count(index) == 0, "Trying to register an index that has already been registered.");
    indexes_.insert(index);
  }

  /**
   * Unregister an index to be periodically garbage collected
   * @param index pointer to the index to unregister
   */
  void UnregisterIndexForGC(index::Index *const index) {
    TERRIER_ASSERT(index != nullptr, "Index cannot be nullptr.");
    common::SharedLatch::ScopedExclusiveLatch guard(&indexes_latch_);
    TERRIER_ASSERT(indexes_.count(index) == 1, "Trying to unregister an index that has not been registered.");
    indexes_.erase(index);
  }

 private:
  volatile bool run_gc_;
  volatile bool gc_paused_;
  storage::GarbageCollector gc_;
  std::chrono::milliseconds gc_period_;
  std::thread gc_thread_;

  std::unordered_set<index::Index *> indexes_;
  common::SharedLatch indexes_latch_;

  void GCThreadLoop() {
    while (run_gc_) {
      std::this_thread::sleep_for(gc_period_);
      if (!gc_paused_) gc_.PerformGarbageCollection();
      common::SharedLatch::ScopedSharedLatch guard(&indexes_latch_);
      for (const auto &index : indexes_) index->PerformGarbageCollection();
    }
  }
};

}  // namespace terrier::storage
