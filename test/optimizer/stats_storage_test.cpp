#include "optimizer/statistics/stats_storage.h"

#include <utility>

#include "gtest/gtest.h"
#include "test_util/test_harness.h"

namespace noisepage::optimizer {
class StatsStorageTests : public TerrierTest {
 protected:
  TableStats table_stats_obj_;
  StatsStorage stats_storage_;

  void SetUp() override {}
};

// NOLINTNEXTLINE
TEST_F(StatsStorageTests, GetTableStatsTest) {}

}  // namespace noisepage::optimizer
