#include "metric/stats_aggregator.h"

#include <vector>

namespace terrier::metric {

std::vector<AbstractRawData *> StatsAggregator::AggregateRawData() {
  std::vector<AbstractRawData *> acc;
  auto collector_map = ThreadLevelStatsCollector::GetAllCollectors();
  for (auto iter = collector_map.Begin(); iter != collector_map.End(); ++iter) {
    auto data_block = iter->second->GetDataToAggregate();
    if (acc.empty()) {
      acc = data_block;
    } else {
      for (size_t i = 0; i < data_block.size(); i++) {
        acc[i]->Aggregate(data_block[i]);
        delete data_block[i];
      }
    }
  }
  return acc;
}

void StatsAggregator::Aggregate() {
  auto acc = AggregateRawData();
  for (auto raw_data : acc) {
    //    raw_data->UpdateAndPersist(txn_manager_, txn);
    delete raw_data;
  }
}
}  // namespace terrier::metric
