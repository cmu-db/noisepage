#pragma once

#include "gtest/gtest.h"
#include "storage/metric/metric_defs.h"
#include "storage/metric/stats_aggregator.h"
#include "storage/metric/test_metric.h"

namespace terrier {

class TestingStatsUtil {
 public:
  static int AggregateTestCounts();
};
}  // namespace terrier
