#include "metric/metrics_manager.h"
#include <memory>
#include <utility>
#include <vector>

namespace terrier::metric {

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
}  // namespace terrier::metric
