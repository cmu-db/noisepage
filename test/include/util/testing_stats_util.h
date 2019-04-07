#pragma once

#include "gtest/gtest.h"
#include "stats/statistic_defs.h"
#include "stats/stats_aggregator.h"
#include "stats/test_metric.h"

namespace terrier {

class TestingStatsUtil {
 public:
  static int AggregateTestCounts();
};
}  // namespace terrier
