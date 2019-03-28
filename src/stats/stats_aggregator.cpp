#include "stats/stats_aggregator.h"

namespace terrier::stats {

void StatsAggregator::Terminate() {
  std::unique_lock<std::mutex> lock(mutex_);
  exiting_ = true;
  while (exiting_) exec_finished_.wait(lock);
}

void StatsAggregator::RunTask() {
  std::unique_lock<std::mutex> lock(mutex_);
  while (exec_finished_.wait_for(lock, std::chrono::milliseconds(aggregation_interval_ms_)) ==
             std::cv_status::timeout &&
         !exiting_)
    Aggregate();
  exiting_ = false;
  exec_finished_.notify_all();
}

using RawDataCollect = std::vector<std::shared_ptr<AbstractRawData>>;
RawDataCollect StatsAggregator::AggregateRawData() {
  RawDataCollect acc = std::vector<std::shared_ptr<AbstractRawData>>();
  for (auto &entry : ThreadLevelStatsCollector::GetAllCollectors()) {
    auto data_block = entry.second.GetDataToAggregate();
    if (acc.empty())
      acc = data_block;
    else
      for (size_t i = 0; i < data_block.size(); i++) {
        acc[i]->Aggregate(*data_block[i]);
      }
  };
  return acc;
}

void StatsAggregator::Aggregate() {
  auto acc = AggregateRawData();
  for (auto &raw_data : acc) {
    raw_data->UpdateAndPersist();
  }
}

}  // namespace terrier::stats
