#include "stats/thread_level_stats_collector.h"
#include "stats/database_metric.h"
#include "stats/statistic_defs.h"
#include "stats/test_metric.h"

namespace terrier::stats {

ThreadLevelStatsCollector::CollectorsMap ThreadLevelStatsCollector::collector_map_ = CollectorsMap();

transaction::TransactionManager *ThreadLevelStatsCollector::txn_manager_ = nullptr;

ThreadLevelStatsCollector::ThreadLevelStatsCollector() {
  collector_map_.Insert(std::this_thread::get_id(), this);
  RegisterMetric<DatabaseMetric>({StatsEventType::TXN_BEGIN, StatsEventType::TXN_COMMIT, StatsEventType::TXN_ABORT});
  RegisterMetric<TestMetric>({StatsEventType::TEST});
}

ThreadLevelStatsCollector::~ThreadLevelStatsCollector() {
  metrics_.clear();
  metric_dispatch_.clear();
  collector_map_.Insert(std::this_thread::get_id(), NULL);
}

std::vector<std::shared_ptr<AbstractRawData>> ThreadLevelStatsCollector::GetDataToAggregate() {
  std::vector<std::shared_ptr<AbstractRawData>> result;
  for (auto &metric : metrics_) result.push_back(metric->Swap());
  return result;
}
}  // namespace terrier::stats
