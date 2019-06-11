#include "metric/stats_aggregator.h"
#include <memory>
#include <utility>
#include <vector>

namespace terrier::metric {

std::vector<std::unique_ptr<AbstractRawData>> StatsAggregator::AggregateRawData() {
  std::vector<std::unique_ptr<AbstractRawData>> acc;
  auto collector_map = ThreadLevelStatsCollector::GetAllCollectors();
  for (auto iter = collector_map.Begin(); iter != collector_map.End(); ++iter) {
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

void StatsAggregator::Aggregate() {
  auto acc = AggregateRawData();
  for (const auto &raw_data UNUSED_ATTRIBUTE : acc) {
    //    raw_data->UpdateAndPersist(txn_manager_, txn);
  }
}
}  // namespace terrier::metric
