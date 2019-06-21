#include "metrics/metrics_manager.h"
#include <memory>
#include <utility>
#include <vector>

namespace terrier::metrics {

void MetricsManager::Aggregate() {
  common::SpinLatch::ScopedSpinLatch guard(&write_latch_);
  for (const auto &metrics_store : stores_map_) {
    auto raw_data = metrics_store.second->GetDataToAggregate();

    for (uint8_t component = 0; component < NUM_COMPONENTS; component++) {
      if (enabled_metrics_.test(component)) {
        if (aggregated_metrics_[component] == nullptr)
          aggregated_metrics_[component] = std::move(raw_data[component]);
        else
          aggregated_metrics_[component]->Aggregate(raw_data[component].get());
      }
    }
  }
}

void MetricsManager::ResetMetric(const MetricsComponent component) const {
  for (const auto &metrics_store : stores_map_) {
    const auto &metric = metrics_store.second->metrics_[static_cast<uint8_t>(component)];
    metric->Swap();
  }
}

/**
 * @return pointer to this thread's MetricsStore object
 */
void MetricsManager::RegisterThread() {
  common::SpinLatch::ScopedSpinLatch guard(&write_latch_);
  const auto thread_id = std::this_thread::get_id();
  TERRIER_ASSERT(stores_map_.count(thread_id) == 0, "This thread was already registered.");
  auto result = stores_map_.emplace(thread_id, new MetricsStore(enabled_metrics_));
  TERRIER_ASSERT(result.second, "Insertion to concurrent map failed.");
  metrics_store_ = result.first->second;
}

/**
 * Should be called by the thread when it is guaranteed to no longer be collecting any more metrics, otherwise,
 * segfault could happen when the unique_ptr releases the MetricsStore
 */
void MetricsManager::UnregisterThread() {
  // TODO(Matt): if we add the notion of thread_local ThreadContext, this should probably be called from that
  // destructor when thread is guaranteed to be in teardown state
  common::SpinLatch::ScopedSpinLatch guard(&write_latch_);
  const auto thread_id = std::this_thread::get_id();
  TERRIER_ASSERT(stores_map_.count(thread_id) == 1, "This thread was never registered.");
  stores_map_.erase(thread_id);
  TERRIER_ASSERT(stores_map_.count(thread_id) == 0, "Deletion from concurrent map failed.");
}

thread_local common::ManagedPointer<MetricsStore> MetricsManager::metrics_store_ = nullptr;

}  // namespace terrier::metrics
