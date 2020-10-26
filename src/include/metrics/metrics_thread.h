#pragma once

#include <chrono>  //NOLINT
#include <thread>  //NOLINT

#include "metrics/metrics_manager.h"

namespace noisepage::metrics {

/**
 * Class for spinning off a thread that runs metrics collection at a fixed interval. This should be used in most cases
 * to enable metrics aggregation in the system unless you need fine-grained control over state or profiling.
 */
class MetricsThread {
 public:
  /**
   * @param metrics_manager pointer to the object to be run on this thread
   * @param metrics_period sleep time between metrics invocations
   */
  MetricsThread(common::ManagedPointer<MetricsManager> metrics_manager,
                const std::chrono::microseconds metrics_period)  // NOLINT
      : metrics_manager_(metrics_manager),
        run_metrics_(true),
        metrics_paused_(false),
        metrics_period_(metrics_period),
        metrics_thread_(std::thread([this] { MetricsThreadLoop(); })) {}

  ~MetricsThread() {
    run_metrics_ = false;
    metrics_thread_.join();
    metrics_manager_->ToCSV();
  }

  /**
   * Pause the metrics from running, typically for use in tests when the state needs to be fixed.
   */
  void PauseMetrics() {
    NOISEPAGE_ASSERT(!metrics_paused_, "Metrics should not already be paused.");
    metrics_paused_ = true;
  }

  /**
   * Resume metrics after being paused.
   */
  void ResumeMetrics() {
    NOISEPAGE_ASSERT(metrics_paused_, "Metrics should already be paused.");
    metrics_paused_ = false;
  }

  /**
   * @return the underlying metrics manager object.
   */
  common::ManagedPointer<metrics::MetricsManager> GetMetricsManager() { return metrics_manager_; }

 private:
  const common::ManagedPointer<metrics::MetricsManager> metrics_manager_;
  volatile bool run_metrics_;
  volatile bool metrics_paused_;
  std::chrono::microseconds metrics_period_;
  std::thread metrics_thread_;

  void MetricsThreadLoop() {
    while (run_metrics_) {
      std::this_thread::sleep_for(metrics_period_);
      if (!metrics_paused_) {
        metrics_manager_->Aggregate();
        metrics_manager_->ToCSV();
      }
    }
  }
};

}  // namespace noisepage::metrics
