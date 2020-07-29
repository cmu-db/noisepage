#pragma once

#include <chrono>  //NOLINT
#include <thread>  //NOLINT

#include "storage/garbage_collector.h"
#include "storage/write_ahead_log/log_manager.h"
#include "transaction/deferred_action_manager.h"

namespace terrier::metrics {
class MetricsManager;
}

namespace terrier::storage {
constexpr uint8_t NUM_GC_THREADS = 2;

/**
 * Class for spinning off a thread that runs garbage collection at a fixed interval. This should be used in most cases
 * to enable GC in the system unless you need fine-grained control over table state or profiling.
 *
 * TODO(John) Repurpose thisto be the deferred action manager's thread
 */
class GarbageCollectorThread {
 public:
  /**
   * @param gc pointer to the garbage collector object to be run on this thread
   * @param gc_period sleep time between GC invocations
   * @param metrics_manager Metrics Manager
   */
  GarbageCollectorThread(common::ManagedPointer<GarbageCollector> gc, std::chrono::milliseconds gc_period,
                         common::ManagedPointer<metrics::MetricsManager> metrics_manager, common::ManagedPointer<storage::LogManager> log_manager = nullptr);

  ~GarbageCollectorThread() { StopGC(); }

  /**
   * Kill the GC thread and run GC a few times to clean up the system.
   */
  void StopGC() {
    TERRIER_ASSERT(run_gc_, "GC should already be running.");
    run_gc_ = false;
    gc_paused_ = false;
    for (auto & gc_thread : gc_threads_) {
      if (gc_thread.joinable()) gc_thread.join();
    }
  }

  /**
   * Spawn the GC thread if it has been previously stopped.
   */
  void StartGC() {
    TERRIER_ASSERT(!run_gc_, "GC should not already be running.");
    run_gc_ = true;
    gc_paused_ = false;
    for (size_t i = 0; i < NUM_GC_THREADS; i++) {
      gc_threads_.emplace_back(std::thread([this, i] {
        if (metrics_manager_ != DISABLED) metrics_manager_->RegisterThread();
        GCThreadLoop(i == 0);
      }));
    }  }

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
  common::ManagedPointer<GarbageCollector> GetGarbageCollector() { return gc_; }

 private:
  const common::ManagedPointer<storage::GarbageCollector> gc_;
  const common::ManagedPointer<metrics::MetricsManager> metrics_manager_;
  const common::ManagedPointer<storage::LogManager> log_manager_;
  volatile bool run_gc_;
  volatile bool gc_paused_;
  std::chrono::milliseconds gc_period_;
  std::vector<std::thread> gc_threads_;

  void GCThreadLoop(bool main_thread) {
    while (run_gc_) {
      std::this_thread::sleep_for(gc_period_);
      if (!gc_paused_) gc_->PerformGarbageCollection(main_thread);
    }
    if (!gc_paused_) {
      gc_->deferred_action_manager_->FullyPerformGC(gc_, log_manager_, main_thread);
    }
  }
};

}  // namespace terrier::storage
