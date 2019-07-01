#pragma once

#include <memory>
#include <thread>  // NOLINT
#include <unordered_map>
#include <vector>

#include "common/managed_pointer.h"
#include "common/spin_latch.h"
#include "common/thread_context.h"
#include "metrics/abstract_raw_data.h"
#include "metrics/metrics_store.h"

namespace terrier::settings {
class Callbacks;
}

namespace terrier::metrics {

/**
 * Background thread that periodically collects data from thread level collectors
 */
class MetricsManager {
 public:
  /**
   * Aggregate metrics from all threads which have collected stats,
   * combine with what was previously persisted in internal SQL tables
   * and insert new total into SQLtable.
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
  const std::array<std::unique_ptr<AbstractRawData>, NUM_COMPONENTS> &AggregatedMetrics() const {
    return aggregated_metrics_;
  }

  /**
   * @param component to be tested
   * @return true if metrics are enabled for this metric, false otherwise
   */
  bool ComponentEnabled(const MetricsComponent component) {
    return enabled_metrics_.test(static_cast<uint8_t>(component));
  }

  void ToCSV() const;

 private:
  friend class settings::Callbacks;

  void EnableMetric(const MetricsComponent component) {
    // overly conservative if this is only called from SettingsManager?
    common::SpinLatch::ScopedSpinLatch guard(&write_latch_);
    TERRIER_ASSERT(!(enabled_metrics_.test(static_cast<uint8_t>(component))), "Metric is already enabled.");

    ResetMetric(component);
    enabled_metrics_.set(static_cast<uint8_t>(component), true);
  }
  void DisableMetric(const MetricsComponent component) {
    common::SpinLatch::ScopedSpinLatch guard(&write_latch_);
    TERRIER_ASSERT(enabled_metrics_.test(static_cast<uint8_t>(component)), "Metric is already disabled.");
    enabled_metrics_.set(static_cast<uint8_t>(component), false);
    aggregated_metrics_[static_cast<uint8_t>(component)].reset(nullptr);
  }

  void ResetMetric(MetricsComponent component) const;

  mutable common::SpinLatch write_latch_;
  std::unordered_map<std::thread::id, std::unique_ptr<MetricsStore>> stores_map_;

  std::array<std::unique_ptr<AbstractRawData>, NUM_COMPONENTS> aggregated_metrics_;

  std::bitset<NUM_COMPONENTS> enabled_metrics_ = 0x0;
};

}  // namespace terrier::metrics
