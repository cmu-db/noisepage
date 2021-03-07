#include "optimizer/statistics/selectivity_util.h"

#include <memory>

#include "execution/sql/value.h"
#include "gtest/gtest.h"
#include "test_util/test_harness.h"

namespace noisepage::optimizer {
class StatsCalculatorTests : public TerrierTest {
 protected:
  void SetUp() override {}
};

// NOLINTNEXTLINE
TEST_F(StatsCalculatorTests, TestSomethingHelp) {}

}  // namespace noisepage::optimizer
