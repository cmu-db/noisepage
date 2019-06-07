#include "metric/thread_level_stats_collector.h"
#include <memory>
#include <vector>
#include "metric/transaction_metric.h"
#include "metric/database_metric.h"
#include "metric/metric_defs.h"
#include "metric/test_metric.h"

namespace terrier::storage::metric {

ThreadLevelStatsCollector::CollectorsMap ThreadLevelStatsCollector::collector_map_ = CollectorsMap();

ThreadLevelStatsCollector::ThreadLevelStatsCollector() {
  thread_id_ = std::this_thread::get_id();
  collector_map_.Insert(thread_id_, this);
  RegisterMetric<DatabaseMetric>({StatsEventType::TXN_BEGIN, StatsEventType::TXN_COMMIT, StatsEventType::TXN_ABORT});
  RegisterMetric<TransactionMetric>({StatsEventType::TXN_BEGIN, StatsEventType::TXN_COMMIT, StatsEventType::TXN_ABORT,
                                     StatsEventType::TUPLE_READ, StatsEventType::TUPLE_UPDATE,
                                     StatsEventType::TUPLE_INSERT, StatsEventType::TUPLE_DELETE});
  RegisterMetric<TestMetric>({StatsEventType::TEST});
}

ThreadLevelStatsCollector::~ThreadLevelStatsCollector() {
  metrics_.clear();
  metric_dispatch_.clear();
  collector_map_.UnsafeErase(thread_id_);
}

std::vector<std::shared_ptr<AbstractRawData>> ThreadLevelStatsCollector::GetDataToAggregate() {
  std::vector<std::shared_ptr<AbstractRawData>> result;
  for (auto &metric : metrics_) result.emplace_back(metric->Swap());
  return result;
}
}  // namespace terrier::storage::metric
