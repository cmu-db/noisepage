#include "util/metric_test_util.h"
#include "metric/stats_aggregator.h"
#include "metric/test_metric.h"

namespace terrier::metric {

int TestingStatsUtil::AggregateTestCounts() {
  StatsAggregator aggregator;
  auto result = aggregator.AggregateRawData();

  int count = 0;

  for (const auto &raw_data : result) {
    if (raw_data->GetMetricType() == MetricType::TEST) {
      count = dynamic_cast<TestMetricRawData *>(raw_data.get())->GetCount();
    }
  }
  return count;
}

}  // namespace terrier::metric
