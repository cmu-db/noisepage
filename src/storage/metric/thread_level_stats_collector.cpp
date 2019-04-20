#include "storage/metric/thread_level_stats_collector.h"
#include <storage/metric/transaction_metric.h>
#include <memory>
#include <vector>
#include "storage/metric/database_metric.h"
#include "storage/metric/metric_defs.h"
#include "storage/metric/test_metric.h"

namespace terrier::storage::metric {

ThreadLevelStatsCollector::CollectorsMap ThreadLevelStatsCollector::collector_map_ = CollectorsMap();

ThreadLevelStatsCollector::ThreadLevelStatsCollector() {
  collector_map_.Insert(std::this_thread::get_id(), this);
  RegisterMetric<DatabaseMetric>({StatsEventType::TXN_BEGIN, StatsEventType::TXN_COMMIT, StatsEventType::TXN_ABORT});
  RegisterMetric<TransactionMetric>({});
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
}  // namespace terrier::storage::metric
