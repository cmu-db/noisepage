#include "metrics/metrics_manager.h"
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace terrier::metrics {

void MetricsManager::Aggregate() {
  common::SpinLatch::ScopedSpinLatch guard(&latch_);
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
    switch (static_cast<MetricsComponent>(component)) {
      case MetricsComponent::LOGGING: {
        const auto &metric = metrics_store.second->logging_metric_;
        metric->Swap();
        break;
      }
      case MetricsComponent::TRANSACTION: {
        const auto &metric = metrics_store.second->txn_metric_;
        metric->Swap();
        break;
      }
    }
  }
}

void MetricsManager::RegisterThread() {
  common::SpinLatch::ScopedSpinLatch guard(&latch_);
  const auto thread_id = std::this_thread::get_id();
  TERRIER_ASSERT(stores_map_.count(thread_id) == 0, "This thread was already registered.");
  auto result = stores_map_.emplace(thread_id, new MetricsStore(common::ManagedPointer(this), enabled_metrics_));
  TERRIER_ASSERT(result.second, "Insertion to concurrent map failed.");
  common::thread_context.metrics_store_ = result.first->second;
}

/**
 * Should be called by the thread when it is guaranteed to no longer be collecting any more metrics, otherwise,
 * segfault could happen when the unique_ptr releases the MetricsStore
 */
void MetricsManager::UnregisterThread() {
  common::SpinLatch::ScopedSpinLatch guard(&latch_);
  const auto thread_id = std::this_thread::get_id();
  stores_map_.erase(thread_id);
  TERRIER_ASSERT(stores_map_.count(thread_id) == 0, "Deletion from concurrent map failed.");
  common::thread_context.metrics_store_ = nullptr;
}

void MetricsManager::ToCSV() const {
  common::SpinLatch::ScopedSpinLatch guard(&latch_);
  for (uint8_t component = 0; component < NUM_COMPONENTS; component++) {
    if (enabled_metrics_.test(component) && aggregated_metrics_[component] != nullptr) {
      std::vector<std::ofstream> outfiles;
      switch (static_cast<MetricsComponent>(component)) {
        case MetricsComponent::LOGGING: {
          outfiles.reserve(LoggingMetricRawData::files_.size());
          for (const auto &file : LoggingMetricRawData::files_) {
            outfiles.emplace_back(std::string(file), std::ios_base::out | std::ios_base::app);
          }
          break;
        }
        case MetricsComponent::TRANSACTION: {
          outfiles.reserve(TransactionMetricRawData::files_.size());
          for (const auto &file : TransactionMetricRawData::files_) {
            outfiles.emplace_back(std::string(file), std::ios_base::out | std::ios_base::app);
          }
          break;
        }
      }
      aggregated_metrics_[component]->ToCSV(&outfiles);
      for (auto &file : outfiles) {
        file.close();
      }
    }
  }
}

}  // namespace terrier::metrics
