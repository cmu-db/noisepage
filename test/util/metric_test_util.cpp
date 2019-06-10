#include "util/metric_test_util.h"
#include "metric/stats_aggregator.h"
#include "metric/test_metric.h"

namespace terrier {

int TestingStatsUtil::AggregateTestCounts() {
  storage::metric::StatsAggregator aggregator(nullptr, nullptr);
  auto result = aggregator.AggregateRawData();

  if (result.empty()) return 0;

  for (auto &raw_data : result) {
    if (raw_data->GetMetricType() == storage::metric::MetricType::TEST) {
      return dynamic_cast<storage::metric::TestMetricRawData *>(raw_data)->GetCount();
    }
  }
  return 0;
}

}  // namespace terrier
