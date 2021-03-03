#include "optimizer/statistics/table_stats.h"

#include <memory>

#include "common/json.h"
#include "gtest/gtest.h"
#include "test_util/test_harness.h"

namespace noisepage::optimizer {
class TableStatsTests : public TerrierTest {
 protected:
  TableStats table_stats_obj_;

  void SetUp() override {}
};

// NOLINTNEXTLINE
TEST_F(TableStatsTests, UpdateNumRowsTest) {}

}  // namespace noisepage::optimizer
