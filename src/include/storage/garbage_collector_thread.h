#pragma once

#include <chrono>  //NOLINT
#include <thread>  //NOLINT
#include "di/di_help.h"
#include "storage/garbage_collector.h"

namespace terrier::storage {

/**
 * Class for spinning off a thread that runs garbage collection at a fixed interval. This should be used in most cases
 * to enable GC in the system unless you need fine-grained control over table state or profiling.
 */
class GarbageCollectorThread {
 public:
  DECLARE_ANNOTATION(GC_PERIOD)
  /**
   * @param txn_manager pointer to the txn manager for the GC to communicate with
   * @param gc_period sleep time between GC invocations
   */
  BOOST_DI_INJECT(GarbageCollectorThread, transaction::TransactionManager *txn_manager,
                  (named = GC_PERIOD) std::chrono::milliseconds gc_period)
      : gc_(txn_manager, nullptr),
        run_gc_(true),
        gc_paused_(false),
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
   * @return the underlying GC object, mostly to register indexes currently.
   */
  GarbageCollector &GetGarbageCollector() { return gc_; }

 private:
  storage::GarbageCollector gc_;
  volatile bool run_gc_;
  volatile bool gc_paused_;
  std::chrono::milliseconds gc_period_;
  std::thread gc_thread_;

  void GCThreadLoop() {
    while (run_gc_) {
      std::this_thread::sleep_for(gc_period_);
      if (!gc_paused_) gc_.PerformGarbageCollection();
    }
  }
};

}  // namespace terrier::storage
