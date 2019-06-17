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

  /**
   * @return the Collector for the calling thread
   */
  common::ManagedPointer<MetricsStore> RegisterThread() {
    common::SpinLatch::ScopedSpinLatch guard(&stores_latch_);
    const auto thread_id = std::this_thread::get_id();
    TERRIER_ASSERT(stores_map_.count(thread_id) == 0, "This thread was already registered.");
    auto result = stores_map_.emplace(thread_id, new MetricsStore(enabled_metrics_));
    TERRIER_ASSERT(result.second, "Insertion to concurrent map failed.");
    return common::ManagedPointer(result.first->second);
  }

  /**
   * Remove thread from metrics map and deallocate its metrics store
   */
  void UnregisterThread() {
    common::SpinLatch::ScopedSpinLatch guard(&stores_latch_);
    const auto thread_id = std::this_thread::get_id();
    TERRIER_ASSERT(stores_map_.count(thread_id) == 1, "This thread was never registered.");
    stores_map_.erase(thread_id);
    TERRIER_ASSERT(stores_map_.count(thread_id) == 0, "Deletion from concurrent map failed.");
  }

  /**
   * @return aggregated metrics
   */
  const std::vector<std::unique_ptr<AbstractRawData>> &AggregatedMetrics() const { return aggregated_metrics_; }

  void EnableMetric(const MetricsComponent component) {
    TERRIER_ASSERT(!(enabled_metrics_.test(static_cast<uint8_t>(component))), "Metric is already enabled.");
    enabled_metrics_.set(static_cast<uint8_t>(component), true);
  }
  void DisableMetric(const MetricsComponent component) {
    TERRIER_ASSERT((enabled_metrics_.test(static_cast<uint8_t>(component))), "Metric is already disabled.");
    enabled_metrics_.set(static_cast<uint8_t>(component), false);
  }

 private:
  common::SpinLatch stores_latch_;
  std::unordered_map<std::thread::id, std::unique_ptr<MetricsStore>> stores_map_;

  std::vector<std::unique_ptr<AbstractRawData>> aggregated_metrics_;

  std::bitset<num_components> enabled_metrics_ = 0x0;
};

}  // namespace terrier::metric
