#include "stats/stats_aggregator.h"
#include <memory>
#include <vector>

namespace terrier::stats {

using RawDataCollect = std::vector<std::shared_ptr<AbstractRawData>>;
RawDataCollect StatsAggregator::AggregateRawData() {
  RawDataCollect acc = std::vector<std::shared_ptr<AbstractRawData>>();
  auto collector_map = ThreadLevelStatsCollector::GetAllCollectors();
  for (auto iter = collector_map.Begin(); iter != collector_map.End(); ++iter) {
    auto data_block = iter->second->GetDataToAggregate();
    if (acc.empty()) {
      acc = data_block;
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
  for (auto &raw_data : acc) {
    raw_data->UpdateAndPersist(txn_manager_);
  }
}

}  // namespace terrier::stats
