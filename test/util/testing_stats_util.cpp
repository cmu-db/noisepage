#include "util/testing_stats_util.h"
#include "stats/stats_aggregator.h"
#include "stats/test_metric.h"

namespace terrier {

int TestingStatsUtil::AggregateTestCounts() {
  stats::StatsAggregator aggregator(1);
  auto result = aggregator.AggregateRawData();

  if (result.empty()) return 0;

  for (auto &raw_data : result) {
    if (raw_data->GetMetricType() == stats::MetricType::TEST) {
      return dynamic_cast<stats::TestMetricRawData *>(raw_data.get())->GetCount();
    }
  }
  return 0;
}

}  // namespace terrier
