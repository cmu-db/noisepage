#pragma once

#include "gtest/gtest.h"
#include "metric/metric_defs.h"
#include "metric/stats_aggregator.h"
#include "metric/test_metric.h"

namespace terrier::metric {

class TestingStatsUtil {
 public:
  static int AggregateTestCounts();
};
}  // namespace terrier::metric
