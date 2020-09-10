#include "optimizer/statistics/stats_storage.h"

#include <utility>

#include "gtest/gtest.h"
#include "test_util/test_harness.h"

namespace terrier::optimizer {
class StatsStorageTests : public TerrierTest {
 protected:
  ColumnStats column_stats_obj_1_;
  ColumnStats column_stats_obj_2_;
  ColumnStats column_stats_obj_3_;
  ColumnStats column_stats_obj_4_;
  ColumnStats column_stats_obj_5_;
  TableStats table_stats_obj_;
  StatsStorage stats_storage_;

  void SetUp() override {
    column_stats_obj_1_ = ColumnStats(catalog::db_oid_t(1), catalog::table_oid_t(1), catalog::col_oid_t(1), 5, 4, 0.2,
                                      {3, 4, 5}, {2, 2, 2}, {1.0, 5.0}, true);
    column_stats_obj_2_ = ColumnStats(catalog::db_oid_t(1), catalog::table_oid_t(1), catalog::col_oid_t(2), 5, 4, 0.2,
                                      {3, 4, 5}, {2, 2, 2}, {1.0, 5.0}, true);
    column_stats_obj_3_ = ColumnStats(catalog::db_oid_t(1), catalog::table_oid_t(1), catalog::col_oid_t(3), 5, 4, 0.2,
                                      {3, 4, 5}, {2, 2, 2}, {1.0, 5.0}, true);
    column_stats_obj_4_ = ColumnStats(catalog::db_oid_t(1), catalog::table_oid_t(1), catalog::col_oid_t(4), 5, 4, 0.2,
                                      {3, 4, 5}, {2, 2, 2}, {1.0, 5.0}, true);
    column_stats_obj_5_ = ColumnStats(catalog::db_oid_t(1), catalog::table_oid_t(1), catalog::col_oid_t(5), 5, 4, 0.2,
                                      {3, 4, 5}, {2, 2, 2}, {1.0, 5.0}, true);
    table_stats_obj_ = TableStats(
        catalog::db_oid_t(1), catalog::table_oid_t(1), 5, true,
        {column_stats_obj_1_, column_stats_obj_2_, column_stats_obj_3_, column_stats_obj_4_, column_stats_obj_5_});
    stats_storage_ = StatsStorage();
  }
};

// NOLINTNEXTLINE
TEST_F(StatsStorageTests, GetTableStatsTest) {
  stats_storage_.InsertTableStats(catalog::db_oid_t(1), catalog::table_oid_t(1), std::move(table_stats_obj_));

  EXPECT_FALSE(stats_storage_.GetTableStats(catalog::db_oid_t(1), catalog::table_oid_t(1)) == nullptr);
  ASSERT_EQ(stats_storage_.GetTableStats(catalog::db_oid_t(2), catalog::table_oid_t(1)), nullptr);
}

// NOLINTNEXTLINE
TEST_F(StatsStorageTests, InsertTableStatsTest) {
  ASSERT_EQ(true, stats_storage_.InsertTableStats(catalog::db_oid_t(1), catalog::table_oid_t(1),
                                                  std::move(table_stats_obj_)));
}

// NOLINTNEXTLINE
TEST_F(StatsStorageTests, DeleteTableStatsTest) {
  stats_storage_.InsertTableStats(catalog::db_oid_t(1), catalog::table_oid_t(1), std::move(table_stats_obj_));
  ASSERT_EQ(true, stats_storage_.DeleteTableStats(catalog::db_oid_t(1), catalog::table_oid_t(1)));

  ASSERT_EQ(false, stats_storage_.DeleteTableStats(catalog::db_oid_t(2), catalog::table_oid_t(1)));
}
}  // namespace terrier::optimizer
