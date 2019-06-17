#include "metric/metrics_manager.h"
#include <memory>
#include <utility>
#include <vector>

namespace terrier::metric {

void MetricsManager::Aggregate() {
  common::SpinLatch::ScopedSpinLatch guard(&write_latch_);
  for (const auto &metrics_store : stores_map_) {
    auto raw_data = metrics_store.second->GetDataToAggregate();

    for (uint8_t component = 0; component < num_components; component++) {
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
  // TODO(Matt): this
}
}  // namespace terrier::metric
