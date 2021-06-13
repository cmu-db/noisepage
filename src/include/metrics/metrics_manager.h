#pragma once

#include <bitset>
#include <memory>
#include <thread>  // NOLINT
#include <unordered_map>
#include <vector>

#include "common/managed_pointer.h"
#include "common/spin_latch.h"
#include "common/thread_context.h"
#include "metrics/abstract_raw_data.h"
#include "metrics/metrics_store.h"

namespace noisepage::settings {
class Callbacks;
}

namespace noisepage::task {
class TaskManager;
}

namespace noisepage::metrics {

/**
 * Background thread that periodically collects data from thread level collectors
 */
class MetricsManager {
 public:
  MetricsManager();

  /**
   * Aggregate metrics from all threads which have collected stats, combine with what was previously collected
   *
   * @warning this method should be called before manipulating the worker pool, especially if
   * some of the worker threads are reassigned to tasks other than execution.
   */
  void Aggregate();

  /**
   * Called by the thread to get a MetricsStore object
   */
  void RegisterThread();

  /**
   * Should be called by the thread when it is guaranteed to no longer be collecting any more metrics, otherwise,
   * segfault could happen when the unique_ptr releases the MetricsStore
   */
  void UnregisterThread();

  /**
   * @return the MetricsManager's aggregated metrics. Currently used in tests
   */
  std::array<std::unique_ptr<AbstractRawData>, NUM_COMPONENTS> &AggregatedMetrics() { return aggregated_metrics_; }

  /**
   * @param component to be tested
   * @return true if metrics are enabled for this component, false otherwise
   */
  bool ComponentEnabled(const MetricsComponent component) {
    return enabled_metrics_.test(static_cast<uint8_t>(component));
  }

  /**
   * Output aggregated metrics.
   */
  void ToOutput(common::ManagedPointer<task::TaskManager> task_manager) const;

  /**
   * @param component to be enabled
   */
  void EnableMetric(const MetricsComponent component) {
    common::SpinLatch::ScopedSpinLatch guard(&latch_);
    NOISEPAGE_ASSERT(!ComponentEnabled(component), "Metric is already enabled.");

    ResetMetric(component);
    enabled_metrics_.set(static_cast<uint8_t>(component), true);
  }

  /**
   * @param component to update
   * @param sample_rate sampling rate of 0 to 100, expressed as a percentage of data points. 100 means all data, 0 none
   */
  void SetMetricSampleRate(MetricsComponent component, uint8_t sample_rate);

  /**
   * @param component to be disabled
   */
  void DisableMetric(const MetricsComponent component) {
    common::SpinLatch::ScopedSpinLatch guard(&latch_);
    NOISEPAGE_ASSERT(ComponentEnabled(component), "Metric is already disabled.");
    enabled_metrics_.set(static_cast<uint8_t>(component), false);
    aggregated_metrics_[static_cast<uint8_t>(component)].reset(nullptr);
  }

  /**
   * Updates the output type of a specific metric component
   * @param component to change
   * @param output of the component
   */
  void SetMetricOutput(const MetricsComponent component, MetricsOutput output) {
    common::SpinLatch::ScopedSpinLatch guard(&latch_);
    metrics_output_[static_cast<uint8_t>(component)] = output;
  }

  /**
   * Retrieves the output type of a specific metric component
   * @param component whose output to retrieve
   * @return output type of the specified component
   */
  metrics::MetricsOutput GetMetricOutput(const MetricsComponent component) {
    common::SpinLatch::ScopedSpinLatch guard(&latch_);
    return metrics_output_[static_cast<uint8_t>(component)];
  }

 private:
  /**
   * Dump aggregated metrics to CSV files.
   */
  void ToCSV(uint8_t component) const;

  /**
   * Dump aggregated metrics to internal tables.
   */
  void ToDB(uint8_t component, common::ManagedPointer<task::TaskManager> task_manager) const;

  void ResetMetric(MetricsComponent component) const;

  mutable common::SpinLatch latch_;
  std::unordered_map<std::thread::id, std::unique_ptr<MetricsStore>> stores_map_;

  std::array<std::unique_ptr<AbstractRawData>, NUM_COMPONENTS> aggregated_metrics_;

  std::bitset<NUM_COMPONENTS> enabled_metrics_ = 0x0;

  std::array<std::vector<bool>, NUM_COMPONENTS> samples_mask_;  // std::vector<bool> may use a bitset for efficiency
  std::array<MetricsOutput, NUM_COMPONENTS> metrics_output_;
};

}  // namespace noisepage::metrics
