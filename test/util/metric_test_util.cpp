#include "util/metric_test_util.h"
#include "metric/stats_aggregator.h"
#include "metric/test_metric.h"

namespace terrier {

int TestingStatsUtil::AggregateTestCounts() {
  storage::metric::StatsAggregator aggregator(nullptr, nullptr);
  auto result = aggregator.AggregateRawData();

  int count = 0;

  for (auto raw_data : result) {
    if (raw_data->GetMetricType() == storage::metric::MetricType::TEST) {
      count = dynamic_cast<storage::metric::TestMetricRawData *>(raw_data)->GetCount();
    }
    delete raw_data;
  }
  return count;
}

}  // namespace terrier
