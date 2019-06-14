#include "metric/metrics_manager.h"
#include <memory>
#include <utility>
#include <vector>

namespace terrier::metric {

void MetricsManager::Aggregate() {
  common::SpinLatch::ScopedSpinLatch guard(&stores_latch_);
  for (const auto &iter : stores_map_) {
    auto data_block = iter.second->GetDataToAggregate();
    if (aggregated_metrics_.empty()) {
      aggregated_metrics_ = std::move(data_block);
    } else {
      for (size_t i = 0; i < data_block.size(); i++) {
        aggregated_metrics_[i]->Aggregate(data_block[i].get());
      }
    }
  }
}
}  // namespace terrier::metric
