#pragma once

#include <chrono>  //NOLINT
#include <thread>  //NOLINT
#include <vector>

#include "metrics/metrics_manager.h"
#include "storage/garbage_collector.h"
#include "storage/write_ahead_log/log_manager.h"
#include "transaction/deferred_action_manager.h"

namespace noisepage::metrics {
class MetricsManager;
}

namespace noisepage::storage {

/**
 * Class for spinning off a thread that runs garbage collection at a fixed interval. This should be used in most cases
 * to enable GC in the system unless you need fine-grained control over table state or profiling.
 *
 * TODO(John) Repurpose this to be the deferred action manager's thread
 */
class GarbageCollectorThread {
 public:
  /**
   * @param gc pointer to the garbage collector object to be run on this thread
   * @param gc_period sleep time between GC invocations
   * @param metrics_manager pointer to the metrics manager
   * @param num_daf_threads number of DAF threads
   */
  GarbageCollectorThread(common::ManagedPointer<GarbageCollector> gc, std::chrono::microseconds gc_period,
                         common::ManagedPointer<metrics::MetricsManager> metrics_manager,
                         uint32_t num_daf_threads = DEFAULT_NUM_DAF_THREADS);

  ~GarbageCollectorThread() { StopGC(); }

  /**
   * Kill the GC thread and run GC a few times to clean up the system.
   */
  void StopGC() {
    NOISEPAGE_ASSERT(run_gc_, "GC should already be running.");
    run_gc_ = false;
    gc_paused_ = false;
    for (auto &gc_thread : gc_threads_) {
      if (gc_thread.joinable()) gc_thread.join();
    }
    for (uint8_t i = 0; i < transaction::MIN_GC_INVOCATIONS; i++) {
      // Using only current thread, process deferred actions without number limit for each run
      gc_->PerformGarbageCollection(false);
    }
  }

  /**
   * Spawn the GC thread if it has been previously stopped.
   */
  void StartGC() {
    NOISEPAGE_ASSERT(!run_gc_, "GC should not already be running.");
    run_gc_ = true;
    gc_paused_ = false;
    for (size_t i = 0; i < num_gc_threads_; i++) {
      gc_threads_.emplace_back(std::thread([this] {
        if (metrics_manager_ != DISABLED) metrics_manager_->RegisterThread();
        GCThreadLoop();
      }));
    }
  }

  /**
   * Pause the GC from running, typically for use in tests when the state of tables need to be fixed.
   */
  void PauseGC() {
    NOISEPAGE_ASSERT(!gc_paused_, "GC should not already be paused.");
    gc_paused_ = true;
  }

  /**
   * Resume GC after being paused.
   */
  void ResumeGC() {
    NOISEPAGE_ASSERT(gc_paused_, "GC should already be paused.");
    gc_paused_ = false;
  }

  /**
   * @return the underlying GC object, mostly to register indexes currently.
   */
  common::ManagedPointer<GarbageCollector> GetGarbageCollector() { return gc_; }

 private:
  const common::ManagedPointer<storage::GarbageCollector> gc_;
  const common::ManagedPointer<metrics::MetricsManager> metrics_manager_;
  volatile bool run_gc_;
  volatile bool gc_paused_;
  std::chrono::microseconds gc_period_;
  std::vector<std::thread> gc_threads_;
  std::uint32_t num_gc_threads_;

  void GCThreadLoop() {
    while (run_gc_) {
      std::this_thread::sleep_for(gc_period_);
      // process deferred actions with number limit per run
      if (!gc_paused_) gc_->PerformGarbageCollection(true);
    }
  }
};

}  // namespace noisepage::storage
