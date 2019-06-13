#pragma once

#include <memory>
#include <vector>

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

  ~MetricsManager() {
    for (auto iter = stores_map_.Begin(); iter != stores_map_.End(); ++iter) {
      auto *const metrics_store = iter->second;
      delete metrics_store;
    }
  }

  /**
   * @return the Collector for the calling thread
   */
  MetricsStore *const RegisterThread() {
    const auto thread_id = std::this_thread::get_id();
    TERRIER_ASSERT(stores_map_.Find(thread_id) == stores_map_.End(), "This thread was already registered.");
    auto *const metrics_store = new MetricsStore(thread_id);
    auto result UNUSED_ATTRIBUTE = stores_map_.Insert(thread_id, metrics_store);
    TERRIER_ASSERT(result.second, "Insertion to concurrent map failed.");
    return metrics_store;
  }

  /**
   * Remove thread from metrics map and deallocate its metrics store
   */
  void UnregisterThread() {
    const auto thread_id = std::this_thread::get_id();
    const auto metrics_store_it = stores_map_.Find(thread_id);
    TERRIER_ASSERT(metrics_store_it != stores_map_.End(), "This thread was never registered.");
    auto *const metrics_store = metrics_store_it->second;
    stores_map_.UnsafeErase(thread_id);
    TERRIER_ASSERT(stores_map_.Find(thread_id) == stores_map_.End(), "Deletion from concurrent map failed.");
    delete metrics_store;
  }

  /**
   * Worker method for Aggregate() that performs stats collection
   * @return raw data collected from all threads
   */
  std::vector<std::unique_ptr<AbstractRawData>> AggregateRawData();

 private:
  /**
   * Concurrent unordered map between thread ID and pointer to an instance of this class
   */
  using StoresMap = common::ConcurrentMap<std::thread::id, MetricsStore *const, std::hash<std::thread::id>>;

  StoresMap stores_map_;
};

}  // namespace terrier::metric
