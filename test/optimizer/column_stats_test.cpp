#include "optimizer/statistics/column_stats.h"

#include "common/json.h"
#include "gtest/gtest.h"
#include "test_util/test_harness.h"

namespace terrier::optimizer {
class ColumnStatsTests : public TerrierTest {
 protected:
  ColumnStats column_stats_obj_;

  void SetUp() override {
    column_stats_obj_ = ColumnStats(catalog::db_oid_t(1), catalog::table_oid_t(1), catalog::col_oid_t(1), 10, 4, 0.2,
                                    {3, 4, 5}, {2, 2, 2}, {1.0, 5.0}, true);
  };
};

// NOLINTNEXTLINE
TEST_F(ColumnStatsTests, GetColumnIDTest) { EXPECT_EQ(catalog::col_oid_t(1), column_stats_obj_.GetColumnID()); }

// NOLINTNEXTLINE
TEST_F(ColumnStatsTests, GetNumRowsTest) { EXPECT_EQ(10, column_stats_obj_.GetNumRows()); }

// NOLINTNEXTLINE
TEST_F(ColumnStatsTests, GetCardinalityTest) { EXPECT_EQ(4, column_stats_obj_.GetCardinality()); }

// NOLINTNEXTLINE
TEST_F(ColumnStatsTests, ColumnStatsJsonTest) {
  auto column_stats_obj_json = column_stats_obj_.ToJson();
  EXPECT_FALSE(column_stats_obj_json.is_null());

  ColumnStats deserialized_column_stats_obj;
  deserialized_column_stats_obj.FromJson(column_stats_obj_json);
  EXPECT_EQ(column_stats_obj_.GetColumnID(), deserialized_column_stats_obj.GetColumnID());
}
}  // namespace terrier::optimizer
