#include "optimizer/statistics/stats_storage.h"

#include "gtest/gtest.h"
#include "storage/sql_table.h"
#include "test_util/end_to_end_test.h"
#include "test_util/test_harness.h"

namespace noisepage::optimizer {
class StatsStorageTests : public test::EndToEndTest {
 protected:
  std::string table_name_ = "empty_nullable_table";
  catalog::col_oid_t col_oid_ = catalog::col_oid_t(1);

 public:
  void SetUp() override {
    EndToEndTest::SetUp();
    auto exec_ctx = MakeExecCtx();
    GenerateTestTables(exec_ctx.get());
  }
};

// NOLINTNEXTLINE
TEST_F(StatsStorageTests, EmptyTableTest) {
  auto table_oid = accessor_->GetTableOid(table_name_);

  std::vector<catalog::col_oid_t> col_oids{col_oid_};
  EXPECT_NO_THROW(stats_storage_->MarkStatsStale(test_db_oid_, table_oid, col_oids));

  const auto latched_table_stats_reference = stats_storage_->GetTableStats(test_db_oid_, table_oid, accessor_.get());
  const auto &table_stats = latched_table_stats_reference.table_stats_;
  EXPECT_EQ(table_stats.GetNumRows(), 0);
  EXPECT_EQ(table_stats.GetColumnCount(), 1);
  EXPECT_TRUE(table_stats.HasColumnStats(col_oid_));
}

// NOLINTNEXTLINE
TEST_F(StatsStorageTests, NonEmptyTableTest) {
  RunQuery("INSERT INTO " + table_name_ + " VALUES(1), (NULL), (1);");
  RunQuery("ANALYZE " + table_name_ + ";");
  txn_manager_->Commit(test_txn_, transaction::TransactionUtil::EmptyCallback, nullptr);
  test_txn_ = txn_manager_->BeginTransaction();

  auto table_oid = accessor_->GetTableOid(table_name_);

  std::vector<catalog::col_oid_t> col_oids{col_oid_};

  const auto latched_table_stats_reference = stats_storage_->GetTableStats(test_db_oid_, table_oid, accessor_.get());
  const auto &table_stats = latched_table_stats_reference.table_stats_;
  EXPECT_EQ(table_stats.GetNumRows(), 3);
  EXPECT_EQ(table_stats.GetColumnCount(), 1);
  EXPECT_TRUE(table_stats.HasColumnStats(col_oid_));

  auto col_stats = table_stats.GetColumnStats(col_oid_);
  EXPECT_EQ(col_stats->GetNumRows(), 3);
  EXPECT_EQ(col_stats->GetDistinctValues(), 1);
  EXPECT_EQ(col_stats->GetNonNullRows(), 2);
  EXPECT_EQ(col_stats->GetNullRows(), 1);
  EXPECT_DOUBLE_EQ(col_stats->GetFracNull(), 1.0 / 3.0);
  EXPECT_EQ(col_stats->GetColumnID(), col_oid_);
}

// NOLINTNEXTLINE
TEST_F(StatsStorageTests, StaleColumnTest) {
  RunQuery("INSERT INTO " + table_name_ + " VALUES(1), (NULL), (1);");
  RunQuery("ANALYZE " + table_name_ + ";");

  auto table_oid = accessor_->GetTableOid(table_name_);

  std::vector<catalog::col_oid_t> col_oids{col_oid_};

  stats_storage_->GetTableStats(test_db_oid_, table_oid, accessor_.get());

  RunQuery("INSERT INTO " + table_name_ + " VALUES (3);");
  RunQuery("ANALYZE " + table_name_ + ";");
  txn_manager_->Commit(test_txn_, transaction::TransactionUtil::EmptyCallback, nullptr);
  test_txn_ = txn_manager_->BeginTransaction();

  const auto latched_table_stats_reference = stats_storage_->GetTableStats(test_db_oid_, table_oid, accessor_.get());
  const auto &table_stats = latched_table_stats_reference.table_stats_;
  EXPECT_EQ(table_stats.GetNumRows(), 4);
  EXPECT_EQ(table_stats.GetColumnCount(), 1);
  EXPECT_TRUE(table_stats.HasColumnStats(col_oid_));

  auto col_stats = table_stats.GetColumnStats(col_oid_);
  EXPECT_EQ(col_stats->GetNumRows(), 4);
  EXPECT_EQ(col_stats->GetDistinctValues(), 2);
  EXPECT_EQ(col_stats->GetNonNullRows(), 3);
  EXPECT_EQ(col_stats->GetNullRows(), 1);
  EXPECT_DOUBLE_EQ(col_stats->GetFracNull(), 1.0 / 4.0);
  EXPECT_EQ(col_stats->GetColumnID(), col_oid_);
}

// NOLINTNEXTLINE
TEST_F(StatsStorageTests, MultithreadedTest) {
  std::vector<std::string> test_tables{"empty_table", "empty_nullable_table", "empty_table2",
                                       "all_types_empty_nullable_table", "all_types_empty_table"};
  std::vector<catalog::table_oid_t> table_oids;
  table_oids.reserve(5);
  for (const std::string &test_table : test_tables) {
    table_oids.emplace_back(accessor_->GetTableOid(test_table));
  }

  auto forward_traversal = [&]() {
    for (const catalog::table_oid_t &table_oid : table_oids) {
      const auto latched_table_stats_reference =
          stats_storage_->GetTableStats(test_db_oid_, table_oid, accessor_.get());
      const auto &table_stats = latched_table_stats_reference.table_stats_;
      EXPECT_EQ(0, table_stats.GetNumRows());
    }
  };

  auto backward_traversal = [&]() {
    for (auto it = table_oids.rbegin(); it != table_oids.rend(); it++) {
      const auto &table_oid = *it;
      const auto latched_table_stats_reference =
          stats_storage_->GetTableStats(test_db_oid_, table_oid, accessor_.get());
      const auto &table_stats = latched_table_stats_reference.table_stats_;
      EXPECT_EQ(0, table_stats.GetNumRows());
    }
  };

  std::vector<std::thread> threads;
  threads.reserve(10);
  for (int i = 0; i < 5; i++) {
    threads.emplace_back(forward_traversal);
  }
  for (int i = 0; i < 5; i++) {
    threads.emplace_back(backward_traversal);
  }

  for (auto &t : threads) {
    t.join();
  }
}

}  // namespace noisepage::optimizer
