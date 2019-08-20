#include <memory>

#include "gtest/gtest.h"
#include "optimizer/statistics/table_stats.h"

#include "util/test_harness.h"

namespace terrier::optimizer {
class TableStatsTests : public TerrierTest {
 protected:
  ColumnStats column_stats_obj_1;
  ColumnStats column_stats_obj_2;
  ColumnStats column_stats_obj_3;
  ColumnStats column_stats_obj_4;
  ColumnStats column_stats_obj_5;
  TableStats table_stats_obj;

  void SetUp() override {
    TerrierTest::SetUp();

    column_stats_obj_1 = ColumnStats(catalog::db_oid_t(1), catalog::table_oid_t(1), catalog::col_oid_t(1), 5, 4, 0.2,
                                     {3, 4, 5}, {2, 2, 2}, {1.0, 5.0}, true);
    column_stats_obj_2 = ColumnStats(catalog::db_oid_t(1), catalog::table_oid_t(1), catalog::col_oid_t(2), 5, 4, 0.2,
                                     {3, 4, 5}, {2, 2, 2}, {1.0, 5.0}, true);
    column_stats_obj_3 = ColumnStats(catalog::db_oid_t(1), catalog::table_oid_t(1), catalog::col_oid_t(3), 5, 4, 0.2,
                                     {3, 4, 5}, {2, 2, 2}, {1.0, 5.0}, true);
    column_stats_obj_4 = ColumnStats(catalog::db_oid_t(1), catalog::table_oid_t(1), catalog::col_oid_t(4), 5, 4, 0.2,
                                     {3, 4, 5}, {2, 2, 2}, {1.0, 5.0}, true);
    column_stats_obj_5 = ColumnStats(catalog::db_oid_t(1), catalog::table_oid_t(1), catalog::col_oid_t(5), 5, 4, 0.2,
                                     {3, 4, 5}, {2, 2, 2}, {1.0, 5.0}, true);
    table_stats_obj = TableStats(
        catalog::db_oid_t(1), catalog::table_oid_t(1), 5, true,
        {column_stats_obj_1, column_stats_obj_2, column_stats_obj_3, column_stats_obj_4, column_stats_obj_5});
  }

  void TearDown() override { TerrierTest::TearDown(); }
};

// NOLINTNEXTLINE
TEST_F(TableStatsTests, UpdateNumRowsTest) {
  table_stats_obj.UpdateNumRows(table_stats_obj.GetNumRows() + 1);
  ASSERT_EQ(table_stats_obj.GetNumRows(), 6);

  ASSERT_EQ(table_stats_obj.GetColumnStats(catalog::col_oid_t(1))->GetNumRows(), 6);
}

// NOLINTNEXTLINE
TEST_F(TableStatsTests, AddColumnStatsTest) {
  auto column_stats_obj_insert = ColumnStats(catalog::db_oid_t(1), catalog::table_oid_t(1), catalog::col_oid_t(6), 5, 4,
                                             0.2, {3, 4, 5}, {2, 2, 2}, {1.0, 5.0}, true);

  ASSERT_EQ(true, table_stats_obj.AddColumnStats(std::make_unique<ColumnStats>(column_stats_obj_insert)));
  ASSERT_EQ(catalog::col_oid_t(6), (table_stats_obj.GetColumnStats(catalog::col_oid_t(6)))->GetColumnID());
}

// NOLINTNEXTLINE
TEST_F(TableStatsTests, ClearColumnStatsTest) {
  table_stats_obj.ClearColumnStats();
  ASSERT_EQ(table_stats_obj.GetColumnCount(), 0);
}

// NOLINTNEXTLINE
TEST_F(TableStatsTests, GetCardinalityTest) {
  ASSERT_EQ(table_stats_obj.GetCardinality(catalog::col_oid_t(2)), 4);
  ASSERT_EQ(table_stats_obj.GetCardinality(catalog::col_oid_t(6)), 0);
}

// NOLINTNEXTLINE
TEST_F(TableStatsTests, GetColumnCountTest) { ASSERT_EQ(table_stats_obj.GetColumnCount(), 5); }

// NOLINTNEXTLINE
TEST_F(TableStatsTests, HasColumnStatsTest) { ASSERT_EQ(table_stats_obj.HasColumnStats(catalog::col_oid_t(5)), true); }

// NOLINTNEXTLINE
TEST_F(TableStatsTests, GetColumnStatsTest) {
  ASSERT_EQ(table_stats_obj.GetColumnStats(catalog::col_oid_t(5))->GetColumnID(), catalog::col_oid_t(5));
}

// NOLINTNEXTLINE
TEST_F(TableStatsTests, RemoveColumnStatsTest) {
  ASSERT_EQ(table_stats_obj.RemoveColumnStats(catalog::col_oid_t(5)), true);
  ASSERT_EQ(table_stats_obj.GetColumnStats(catalog::col_oid_t(5)), nullptr);
  ASSERT_EQ(table_stats_obj.RemoveColumnStats(catalog::col_oid_t(6)), false);
}

// NOLINTNEXTLINE
TEST_F(TableStatsTests, IsBaseTableTest) { ASSERT_EQ(table_stats_obj.IsBaseTable(), true); }

// NOLINTNEXTLINE
TEST_F(TableStatsTests, GetNumRowsTest) { ASSERT_EQ(table_stats_obj.GetNumRows(), 5); }

// NOLINTNEXTLINE
TEST_F(TableStatsTests, TableStatsJsonTest) {
  auto table_stats_obj_json = table_stats_obj.ToJson();
  EXPECT_FALSE(table_stats_obj_json.is_null());

  TableStats deserialized_table_stats_obj;
  deserialized_table_stats_obj.FromJson(table_stats_obj_json);
  EXPECT_EQ(table_stats_obj.GetNumRows(), deserialized_table_stats_obj.GetNumRows());
}
}  // namespace terrier::optimizer
