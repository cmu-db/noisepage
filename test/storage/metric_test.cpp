#include <random>
#include <unordered_map>
#include "storage/metric/database_metric.h"
#include "storage/metric/stats_aggregator.h"
#include "storage/metric/thread_level_stats_collector.h"
#include "util/test_harness.h"
#include "util/testing_stats_util.h"
#include "util/transaction_test_util.h"

namespace terrier {

/**
 * @brief Test the correctness of database metric
 */
class MetricTests : public TerrierTest {
 public:
  void SetUp() override {
    TerrierTest::SetUp();
    txn_manager_ = new transaction::TransactionManager(&buffer_pool_, true, LOGGING_DISABLED);

    txn_ = txn_manager_->BeginTransaction();
    catalog_ = new catalog::Catalog(txn_manager_, txn_);
  }

  void TearDown() override {
    txn_manager_->Commit(txn_, TestCallbacks::EmptyCallback, nullptr);

    TerrierTest::TearDown();
    delete catalog_;  // need to delete catalog_first
    delete txn_manager_;
    delete txn_;
  }

  catalog::Catalog *catalog_;
  storage::RecordBufferSegmentPool buffer_pool_{100, 100};

  transaction::TransactionContext *txn_ = nullptr;
  transaction::TransactionManager *txn_manager_;
  std::default_random_engine generator_;
  const uint8_t num_iterations_ = 100;
  const uint8_t num_databases_ = 100;
};

/**
 * Basic test for testing database metric registration and stats collection, single thread
 */
// NOLINTNEXTLINE
TEST_F(MetricTests, DatabaseMetricBasicTest) {
  const uint32_t num_threads = MultiThreadTestUtil::HardwareConcurrency();
  common::WorkerPool thread_pool(num_threads, {});
  auto stats_collector = storage::metric::ThreadLevelStatsCollector();
  storage::metric::StatsAggregator aggregator(txn_manager_, catalog_);
  for (uint8_t i = 0; i < num_iterations_; i++) {
    std::unordered_map<uint8_t, int32_t> commit_map;
    std::unordered_map<uint8_t, int32_t> abort_map;
    for (uint8_t j = 0; j < num_databases_; j++) {
      commit_map[j] = 0;
      abort_map[j] = 0;
      auto num_txns_ = static_cast<uint8_t>(std::uniform_int_distribution<uint8_t>(0, UINT8_MAX)(generator_));
      for (uint8_t k = 0; k < num_txns_; k++) {
        auto *txn = txn_manager_->BeginTransaction();
        storage::metric::ThreadLevelStatsCollector::GetCollectorForThread()->CollectTransactionBegin(txn);
        auto txn_res = static_cast<bool>(std::uniform_int_distribution<uint8_t>(0, 1)(generator_));
        if (txn_res) {
          txn_manager_->Commit(txn, TestCallbacks::EmptyCallback, nullptr);
          storage::metric::ThreadLevelStatsCollector::GetCollectorForThread()->CollectTransactionCommit(
              txn, static_cast<catalog::db_oid_t>(j));
          commit_map[j]++;
        } else {
          txn_manager_->Abort(txn);
          storage::metric::ThreadLevelStatsCollector::GetCollectorForThread()->CollectTransactionAbort(
              txn, static_cast<catalog::db_oid_t>(j));
          abort_map[j]++;
        }
      }
    }

    auto result = aggregator.AggregateRawData();
    EXPECT_FALSE(result.empty());

    for (auto &raw_data : result) {
      if (raw_data->GetMetricType() == storage::metric::MetricType::DATABASE) {
        for (uint8_t j = 0; j < num_databases_; j++) {
          auto commit_cnt = dynamic_cast<storage::metric::DatabaseMetricRawData *>(raw_data.get())
                                ->GetCommitCount(static_cast<catalog::db_oid_t>(j));
          auto abort_cnt = dynamic_cast<storage::metric::DatabaseMetricRawData *>(raw_data.get())
                               ->GetAbortCount(static_cast<catalog::db_oid_t>(j));
          EXPECT_EQ(commit_cnt, commit_map[j]);
          EXPECT_EQ(abort_cnt, abort_map[j]);
        }
      }
    }
  }
}


/**
 *  Testing database metric stats collection and persisting, single thread
 */
// NOLINTNEXTLINE
TEST_F(MetricTests, DatabaseMetricStorageTest) {
  
  auto stats_collector = storage::metric::ThreadLevelStatsCollector();
  storage::metric::StatsAggregator aggregator(txn_manager_, catalog_);
  for (uint8_t i = 0; i < num_iterations_; i++) {
    std::unordered_map<uint8_t, int32_t> commit_map;
    std::unordered_map<uint8_t, int32_t> abort_map;
    for (uint8_t j = 0; j < num_databases_; j++) {
      commit_map[j] = 0;
      abort_map[j] = 0;
      auto num_txns_ = static_cast<uint8_t>(std::uniform_int_distribution<uint8_t>(0, UINT8_MAX)(generator_));
      for (uint8_t k = 0; k < num_txns_; k++) {
        auto *txn = txn_manager_->BeginTransaction();
        storage::metric::ThreadLevelStatsCollector::GetCollectorForThread()->CollectTransactionBegin(txn);
        auto txn_res = static_cast<bool>(std::uniform_int_distribution<uint8_t>(0, 1)(generator_));
        if (txn_res) {
          txn_manager_->Commit(txn, TestCallbacks::EmptyCallback, nullptr);
          storage::metric::ThreadLevelStatsCollector::GetCollectorForThread()->CollectTransactionCommit(
              txn, static_cast<catalog::db_oid_t>(j));
          commit_map[j]++;
        } else {
          txn_manager_->Abort(txn);
          storage::metric::ThreadLevelStatsCollector::GetCollectorForThread()->CollectTransactionAbort(
              txn, static_cast<catalog::db_oid_t>(j));
          abort_map[j]++;
        }
      }
    }
    aggregator.Aggregate();

    auto txn = txn_manager_->BeginTransaction();
    const catalog::db_oid_t terrier_oid(catalog::DEFAULT_DATABASE_OID);
    auto db_handle = catalog_->GetDatabaseHandle();
    auto table_handle = db_handle.GetNamespaceHandle(txn, terrier_oid).GetTableHandle(txn, "public");
    auto table = table_handle.GetTable(txn, "database_metric_table");

    for (uint8_t j = 0; j < num_databases_; j++) {
      std::vector<type::TransientValue> search_vec;
      search_vec.emplace_back(type::TransientValueFactory::GetInteger(static_cast<int32_t>(j)));
      auto row = table->FindRow(txn, search_vec);
      auto commit_cnt = type::TransientValuePeeker::PeekInteger(row[1]);
      auto abort_cnt = type::TransientValuePeeker::PeekInteger(row[2]);
      EXPECT_EQ(commit_cnt, commit_map[j]);
      EXPECT_EQ(abort_cnt, abort_map[j]);
    }
    txn_manager_->Commit(txn, TestCallbacks::EmptyCallback, nullptr);
  }
}

}  // namespace terrier
