#include <utility>

#include "gtest/gtest.h"
#include "optimizer/statistics/stats_storage.h"

#include "util/test_harness.h"

namespace terrier::optimizer {

// NOLINTNEXTLINE
TEST(StatsStorageTests, GetTableStatsTest) {
  auto column_stats_obj_1 = ColumnStats(catalog::db_oid_t(1), catalog::table_oid_t(1), catalog::col_oid_t(1), 5, 4, 0.2,
                                        {3, 4, 5}, {2, 2, 2}, {1.0, 5.0}, true);
  auto column_stats_obj_2 = ColumnStats(catalog::db_oid_t(1), catalog::table_oid_t(1), catalog::col_oid_t(2), 5, 4, 0.2,
                                        {3, 4, 5}, {2, 2, 2}, {1.0, 5.0}, true);
  auto column_stats_obj_3 = ColumnStats(catalog::db_oid_t(1), catalog::table_oid_t(1), catalog::col_oid_t(3), 5, 4, 0.2,
                                        {3, 4, 5}, {2, 2, 2}, {1.0, 5.0}, true);
  auto column_stats_obj_4 = ColumnStats(catalog::db_oid_t(1), catalog::table_oid_t(1), catalog::col_oid_t(4), 5, 4, 0.2,
                                        {3, 4, 5}, {2, 2, 2}, {1.0, 5.0}, true);
  auto column_stats_obj_5 = ColumnStats(catalog::db_oid_t(1), catalog::table_oid_t(1), catalog::col_oid_t(5), 5, 4, 0.2,
                                        {3, 4, 5}, {2, 2, 2}, {1.0, 5.0}, true);
  auto table_stats_obj =
      TableStats(catalog::db_oid_t(1), catalog::table_oid_t(1), 5, true,
                 {column_stats_obj_1, column_stats_obj_2, column_stats_obj_3, column_stats_obj_4, column_stats_obj_5});
  auto stats_storage = StatsStorage();
  stats_storage.InsertTableStats(catalog::db_oid_t(1), catalog::table_oid_t(1), std::move(table_stats_obj));
  EXPECT_FALSE(stats_storage.GetTableStats(catalog::db_oid_t(1), catalog::table_oid_t(1)) == nullptr);

  ASSERT_EQ(stats_storage.GetTableStats(catalog::db_oid_t(2), catalog::table_oid_t(1)), nullptr);
}

// NOLINTNEXTLINE
TEST(StatsStorageTests, InsertTableStatsTest) {
  auto column_stats_obj_1 = ColumnStats(catalog::db_oid_t(1), catalog::table_oid_t(1), catalog::col_oid_t(1), 5, 4, 0.2,
                                        {3, 4, 5}, {2, 2, 2}, {1.0, 5.0}, true);
  auto column_stats_obj_2 = ColumnStats(catalog::db_oid_t(1), catalog::table_oid_t(1), catalog::col_oid_t(2), 5, 4, 0.2,
                                        {3, 4, 5}, {2, 2, 2}, {1.0, 5.0}, true);
  auto column_stats_obj_3 = ColumnStats(catalog::db_oid_t(1), catalog::table_oid_t(1), catalog::col_oid_t(3), 5, 4, 0.2,
                                        {3, 4, 5}, {2, 2, 2}, {1.0, 5.0}, true);
  auto column_stats_obj_4 = ColumnStats(catalog::db_oid_t(1), catalog::table_oid_t(1), catalog::col_oid_t(4), 5, 4, 0.2,
                                        {3, 4, 5}, {2, 2, 2}, {1.0, 5.0}, true);
  auto column_stats_obj_5 = ColumnStats(catalog::db_oid_t(1), catalog::table_oid_t(1), catalog::col_oid_t(5), 5, 4, 0.2,
                                        {3, 4, 5}, {2, 2, 2}, {1.0, 5.0}, true);
  auto table_stats_obj =
      TableStats(catalog::db_oid_t(1), catalog::table_oid_t(1), 5, true,
                 {column_stats_obj_1, column_stats_obj_2, column_stats_obj_3, column_stats_obj_4, column_stats_obj_5});
  auto stats_storage = StatsStorage();
  ASSERT_EQ(true,
            stats_storage.InsertTableStats(catalog::db_oid_t(1), catalog::table_oid_t(1), std::move(table_stats_obj)));
}

// NOLINTNEXTLINE
TEST(StatsStorageTests, DeleteTableStatsTest) {
  auto column_stats_obj_1 = ColumnStats(catalog::db_oid_t(1), catalog::table_oid_t(1), catalog::col_oid_t(1), 5, 4, 0.2,
                                        {3, 4, 5}, {2, 2, 2}, {1.0, 5.0}, true);
  auto column_stats_obj_2 = ColumnStats(catalog::db_oid_t(1), catalog::table_oid_t(1), catalog::col_oid_t(2), 5, 4, 0.2,
                                        {3, 4, 5}, {2, 2, 2}, {1.0, 5.0}, true);
  auto column_stats_obj_3 = ColumnStats(catalog::db_oid_t(1), catalog::table_oid_t(1), catalog::col_oid_t(3), 5, 4, 0.2,
                                        {3, 4, 5}, {2, 2, 2}, {1.0, 5.0}, true);
  auto column_stats_obj_4 = ColumnStats(catalog::db_oid_t(1), catalog::table_oid_t(1), catalog::col_oid_t(4), 5, 4, 0.2,
                                        {3, 4, 5}, {2, 2, 2}, {1.0, 5.0}, true);
  auto column_stats_obj_5 = ColumnStats(catalog::db_oid_t(1), catalog::table_oid_t(1), catalog::col_oid_t(5), 5, 4, 0.2,
                                        {3, 4, 5}, {2, 2, 2}, {1.0, 5.0}, true);
  auto table_stats_obj =
      TableStats(catalog::db_oid_t(1), catalog::table_oid_t(1), 5, true,
                 {column_stats_obj_1, column_stats_obj_2, column_stats_obj_3, column_stats_obj_4, column_stats_obj_5});
  auto stats_storage = StatsStorage();
  stats_storage.InsertTableStats(catalog::db_oid_t(1), catalog::table_oid_t(1), std::move(table_stats_obj));
  ASSERT_EQ(true, stats_storage.DeleteTableStats(catalog::db_oid_t(1), catalog::table_oid_t(1)));

  ASSERT_EQ(false, stats_storage.DeleteTableStats(catalog::db_oid_t(2), catalog::table_oid_t(1)));
}
}  // namespace terrier::optimizer
