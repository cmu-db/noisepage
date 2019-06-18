#pragma once

#include <memory>
#include <unordered_map>
#include <vector>

#include "common/managed_pointer.h"
#include "common/spin_latch.h"
#include "metric/abstract_raw_data.h"
#include "metric/metrics_store.h"

namespace terrier::metric {

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

  common::ManagedPointer<MetricsStore> RegisterThread() {
    common::SpinLatch::ScopedSpinLatch guard(&write_latch_);
    const auto thread_id = std::this_thread::get_id();
    TERRIER_ASSERT(stores_map_.count(thread_id) == 0, "This thread was already registered.");
    auto result = stores_map_.emplace(thread_id, new MetricsStore(enabled_metrics_));
    TERRIER_ASSERT(result.second, "Insertion to concurrent map failed.");
    return common::ManagedPointer(result.first->second);
  }

  /**
   * Should be called by the thread when it is guaranteed to no longer be collecting any more metrics, otherwise,
   * segfault could happen when the unique_ptr releases the MetricsStore
   */
  void UnregisterThread() {
    // TODO(Matt): if we add the notion of thread_local ThreadContext, this should probably be called from that
    // destructor when thread is guaranteed to be in teardown state
    common::SpinLatch::ScopedSpinLatch guard(&write_latch_);
    const auto thread_id = std::this_thread::get_id();
    TERRIER_ASSERT(stores_map_.count(thread_id) == 1, "This thread was never registered.");
    stores_map_.erase(thread_id);
    TERRIER_ASSERT(stores_map_.count(thread_id) == 0, "Deletion from concurrent map failed.");
  }

  const std::array<std::unique_ptr<AbstractRawData>, NUM_COMPONENTS> &AggregatedMetrics() const {
    return aggregated_metrics_;
  }

  void EnableMetric(const MetricsComponent component) {
    // overly conservative if this is only called from SettingsManager?
    common::SpinLatch::ScopedSpinLatch guard(&write_latch_);
    TERRIER_ASSERT(!(enabled_metrics_.test(static_cast<uint8_t>(component))), "Metric is already enabled.");

    ResetMetric(component);
    enabled_metrics_.set(static_cast<uint8_t>(component), true);
  }
  void DisableMetric(const MetricsComponent component) {
    // overly conservative if this is only called from SettingsManager?
    common::SpinLatch::ScopedSpinLatch guard(&write_latch_);
    TERRIER_ASSERT(enabled_metrics_.test(static_cast<uint8_t>(component)), "Metric is already disabled.");
    aggregated_metrics_[static_cast<uint8_t>(component)].reset(nullptr);
    enabled_metrics_.set(static_cast<uint8_t>(component), false);
  }

 private:
  void ResetMetric(MetricsComponent component) const;

  common::SpinLatch write_latch_;
  std::unordered_map<std::thread::id, std::unique_ptr<MetricsStore>> stores_map_;

  std::array<std::unique_ptr<AbstractRawData>, NUM_COMPONENTS> aggregated_metrics_;

  std::bitset<NUM_COMPONENTS> enabled_metrics_ = 0x0;
};

}  // namespace terrier::metric
