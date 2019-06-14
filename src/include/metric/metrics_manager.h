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
    auto result = stores_map_.emplace(thread_id, new MetricsStore());
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

 private:
  common::SpinLatch stores_latch_;
  std::unordered_map<std::thread::id, std::unique_ptr<MetricsStore>> stores_map_;

  std::vector<std::unique_ptr<AbstractRawData>> aggregated_metrics_;
};

}  // namespace terrier::metric
