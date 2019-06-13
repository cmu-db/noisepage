#include "metric/metrics_manager.h"
#include <memory>
#include <utility>
#include <vector>

namespace terrier::metric {

std::vector<std::unique_ptr<AbstractRawData>> MetricsManager::AggregateRawData() {
  std::vector<std::unique_ptr<AbstractRawData>> acc;
  for (auto iter = stores_map_.Begin(); iter != stores_map_.End(); ++iter) {
    auto data_block = iter->second->GetDataToAggregate();
    if (acc.empty()) {
      acc = std::move(data_block);
    } else {
      for (size_t i = 0; i < data_block.size(); i++) {
        acc[i]->Aggregate(data_block[i].get());
      }
    }
  }
  return acc;
}

void MetricsManager::Aggregate() {
  auto acc = AggregateRawData();
  for (const auto &raw_data UNUSED_ATTRIBUTE : acc) {
    //    raw_data->UpdateAndPersist(txn_manager_, txn);
  }
}
}  // namespace terrier::metric
