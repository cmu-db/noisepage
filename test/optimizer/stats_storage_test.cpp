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

  auto table_stats = stats_storage_->GetTableStats(test_db_oid_, table_oid, accessor_.get());
  EXPECT_EQ(table_stats->GetNumRows(), 0);
  EXPECT_EQ(table_stats->GetColumnCount(), 1);
  EXPECT_TRUE(table_stats->HasColumnStats(col_oid_));

  auto col_stats = stats_storage_->GetColumnStats(test_db_oid_, table_oid, col_oid_, accessor_.get());
  EXPECT_EQ(col_stats->GetNumRows(), 0);
  EXPECT_EQ(col_stats->GetDistinctValues(), 0);
  EXPECT_EQ(col_stats->GetNonNullRows(), 0);
  EXPECT_EQ(col_stats->GetNullRows(), 0);
  EXPECT_EQ(col_stats->GetFracNull(), 0.0);
  EXPECT_EQ(col_stats->GetColumnID(), col_oid_);
}

// NOLINTNEXTLINE
TEST_F(StatsStorageTests, NonEmptyTableTest) {
  RunQuery("INSERT INTO " + table_name_ + " VALUES(1), (NULL), (1);");
  RunQuery("ANALYZE " + table_name_ + ";");
  txn_manager_->Commit(test_txn_, transaction::TransactionUtil::EmptyCallback, nullptr);
  test_txn_ = txn_manager_->BeginTransaction();

  auto table_oid = accessor_->GetTableOid(table_name_);

  std::vector<catalog::col_oid_t> col_oids{col_oid_};

  auto table_stats = stats_storage_->GetTableStats(test_db_oid_, table_oid, accessor_.get());
  EXPECT_EQ(table_stats->GetNumRows(), 3);
  EXPECT_EQ(table_stats->GetColumnCount(), 1);
  EXPECT_TRUE(table_stats->HasColumnStats(col_oid_));

  auto col_stats = table_stats->GetColumnStats(col_oid_);
  EXPECT_EQ(col_stats->GetNumRows(), 3);
  EXPECT_EQ(col_stats->GetDistinctValues(), 1);
  EXPECT_EQ(col_stats->GetNonNullRows(), 2);
  EXPECT_EQ(col_stats->GetNullRows(), 1);
  EXPECT_DOUBLE_EQ(col_stats->GetFracNull(), 1.0 / 3.0);
  EXPECT_EQ(col_stats->GetColumnID(), col_oid_);

  auto col_stats_solo = stats_storage_->GetColumnStats(test_db_oid_, table_oid, col_oid_, accessor_.get());
  EXPECT_EQ(col_stats_solo->GetNumRows(), 3);
  EXPECT_EQ(col_stats_solo->GetDistinctValues(), 1);
  EXPECT_EQ(col_stats_solo->GetNonNullRows(), 2);
  EXPECT_EQ(col_stats_solo->GetNullRows(), 1);
  EXPECT_DOUBLE_EQ(col_stats_solo->GetFracNull(), 1.0 / 3.0);
  EXPECT_EQ(col_stats_solo->GetColumnID(), col_oid_);
}

// NOLINTNEXTLINE
TEST_F(StatsStorageTests, StaleColumnTest) {
  RunQuery("INSERT INTO " + table_name_ + " VALUES(1), (NULL), (1);");
  RunQuery("ANALYZE " + table_name_ + ";");
  txn_manager_->Commit(test_txn_, transaction::TransactionUtil::EmptyCallback, nullptr);
  test_txn_ = txn_manager_->BeginTransaction();

  auto table_oid = accessor_->GetTableOid(table_name_);

  std::vector<catalog::col_oid_t> col_oids{col_oid_};

  auto table_stats = stats_storage_->GetTableStats(test_db_oid_, table_oid, accessor_.get());
  auto col_stats_solo = stats_storage_->GetColumnStats(test_db_oid_, table_oid, col_oid_, accessor_.get());

  RunQuery("INSERT INTO " + table_name_ + " VALUES (3);");
  RunQuery("ANALYZE " + table_name_ + ";");
  txn_manager_->Commit(test_txn_, transaction::TransactionUtil::EmptyCallback, nullptr);
  test_txn_ = txn_manager_->BeginTransaction();

  // TODO(Joe) Without this sleep, the index lookup on pg_statistic that stats_storage does comes back empty.
  std::this_thread::sleep_for(std::chrono::seconds(1));

  table_stats = stats_storage_->GetTableStats(test_db_oid_, table_oid, accessor_.get());
  EXPECT_EQ(table_stats->GetNumRows(), 4);
  EXPECT_EQ(table_stats->GetColumnCount(), 1);
  EXPECT_TRUE(table_stats->HasColumnStats(col_oid_));

  auto col_stats = table_stats->GetColumnStats(col_oid_);
  EXPECT_EQ(col_stats->GetNumRows(), 4);
  EXPECT_EQ(col_stats->GetDistinctValues(), 2);
  EXPECT_EQ(col_stats->GetNonNullRows(), 3);
  EXPECT_EQ(col_stats->GetNullRows(), 1);
  EXPECT_DOUBLE_EQ(col_stats->GetFracNull(), 1.0 / 4.0);
  EXPECT_EQ(col_stats->GetColumnID(), col_oid_);

  col_stats_solo = stats_storage_->GetColumnStats(test_db_oid_, table_oid, col_oid_, accessor_.get());
  EXPECT_EQ(col_stats_solo->GetNumRows(), 4);
  EXPECT_EQ(col_stats_solo->GetDistinctValues(), 2);
  EXPECT_EQ(col_stats_solo->GetNonNullRows(), 3);
  EXPECT_EQ(col_stats_solo->GetNullRows(), 1);
  EXPECT_DOUBLE_EQ(col_stats_solo->GetFracNull(), 1.0 / 4.0);
  EXPECT_EQ(col_stats_solo->GetColumnID(), col_oid_);
}

}  // namespace noisepage::optimizer
