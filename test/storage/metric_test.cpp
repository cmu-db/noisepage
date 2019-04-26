#include <random>
#include <unordered_map>
#include <vector>
#include "storage/metric/database_metric.h"
#include "storage/metric/stats_aggregator.h"
#include "storage/metric/thread_level_stats_collector.h"
#include "storage/metric/transaction_metric.h"
#include "transaction/transaction_defs.h"
#include "transaction/transaction_manager.h"
#include "util/metric_test_util.h"
#include "util/test_harness.h"
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
    delete txn_;
    delete txn_manager_;
  }

  catalog::Catalog *catalog_;
  storage::RecordBufferSegmentPool buffer_pool_{100, 100};

  transaction::TransactionContext *txn_ = nullptr;
  transaction::TransactionManager *txn_manager_;
  std::default_random_engine generator_;
  const uint8_t num_iterations_ = 2;
  const uint8_t num_databases_ = 2;
  const uint8_t num_txns_ = 2;
  const int64_t acc_err = 5;
};

/**
 * Basic test for testing metric registration and stats collection
 */
// NOLINTNEXTLINE
TEST_F(MetricTests, BasicTest) {
  storage::metric::StatsAggregator aggregator(txn_manager_, catalog_);
  EXPECT_EQ(aggregator.GetTxnManager(), txn_manager_);
  EXPECT_EQ(aggregator.GetCatalog(), catalog_);

  auto test_num_1 = std::uniform_int_distribution<int32_t>(0, INT32_MAX)(generator_);
  auto test_num_2 = std::uniform_int_distribution<int32_t>(0, INT32_MAX)(generator_);
  auto stats_collector = storage::metric::ThreadLevelStatsCollector();
  storage::metric::ThreadLevelStatsCollector::GetCollectorForThread()->CollectTestNum(test_num_1);
  storage::metric::ThreadLevelStatsCollector::GetCollectorForThread()->CollectTestNum(test_num_2);

  EXPECT_EQ(TestingStatsUtil::AggregateTestCounts(), test_num_1 + test_num_2);
}

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
  std::unordered_map<uint8_t, int32_t> commit_map;
  std::unordered_map<uint8_t, int32_t> abort_map;
  for (uint8_t j = 0; j < num_databases_; j++) {
    commit_map[j] = 0;
    abort_map[j] = 0;
  }
  for (uint8_t i = 0; i < num_iterations_; i++) {
    auto stats_collector = storage::metric::ThreadLevelStatsCollector();
    for (uint8_t j = 0; j < num_databases_; j++) {
      auto num_txns_ = static_cast<uint8_t>(std::uniform_int_distribution<uint8_t>(1, UINT8_MAX)(generator_));
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
    aggregator.Aggregate(txn_);

    const catalog::db_oid_t terrier_oid(catalog::DEFAULT_DATABASE_OID);
    auto db_handle = catalog_->GetDatabaseHandle();
    auto table_handle = db_handle.GetNamespaceHandle(txn_, terrier_oid).GetTableHandle(txn_, "public");
    auto table = table_handle.GetTable(txn_, "database_metric_table");

    for (uint8_t j = 0; j < num_databases_; j++) {
      std::vector<type::TransientValue> search_vec;
      search_vec.emplace_back(type::TransientValueFactory::GetInteger(static_cast<uint32_t>(j)));
      auto row = table->FindRow(txn_, search_vec);
      auto commit_cnt = type::TransientValuePeeker::PeekInteger(row[1]);
      auto abort_cnt = type::TransientValuePeeker::PeekInteger(row[2]);
      EXPECT_EQ(commit_cnt, commit_map[j]);
      EXPECT_EQ(abort_cnt, abort_map[j]);
    }
  }
}

TEST_F(MetricTests, TransactionMetricBasicTest) {
  auto stats_collector = storage::metric::ThreadLevelStatsCollector();
  storage::metric::StatsAggregator aggregator(txn_manager_, catalog_);

  catalog::table_oid_t table_oid = static_cast<catalog::table_oid_t>(2);  // any value
  const catalog::db_oid_t database_oid(catalog::DEFAULT_DATABASE_OID);

  for (uint8_t i = 0; i < num_iterations_; i++) {
    std::unordered_map<uint8_t, transaction::timestamp_t> id_map;
    std::unordered_map<transaction::timestamp_t, int32_t> read_map;
    std::unordered_map<transaction::timestamp_t, int32_t> update_map;
    std::unordered_map<transaction::timestamp_t, int32_t> insert_map;
    std::unordered_map<transaction::timestamp_t, int32_t> delete_map;
    std::unordered_map<transaction::timestamp_t, int64_t> latency_map;
    for (uint8_t j = 0; j < num_txns_; j++) {
      auto start = std::chrono::high_resolution_clock::now();
      auto *txn = txn_manager_->BeginTransaction();
      auto txn_id = txn->TxnId().load();
      id_map[j] = txn_id;
      read_map[txn_id] = 0;
      update_map[txn_id] = 0;
      insert_map[txn_id] = 0;
      delete_map[txn_id] = 0;

      auto num_ops_ = static_cast<uint8_t>(std::uniform_int_distribution<uint8_t>(1, UINT8_MAX)(generator_));
      for (uint8_t k = 0; k < num_ops_; k++) {
        auto op_type = std::uniform_int_distribution<uint8_t>(0, 3)(generator_);
        if (op_type == 0) {  // Read
          storage::metric::ThreadLevelStatsCollector::GetCollectorForThread()->CollectTupleRead(txn, database_oid,
                                                                                                table_oid);
          read_map[txn_id]++;
        } else if (op_type == 1) {  // Update
          storage::metric::ThreadLevelStatsCollector::GetCollectorForThread()->CollectTupleUpdate(txn, database_oid,
                                                                                                  table_oid);
          update_map[txn_id]++;
        } else if (op_type == 2) {  // Insert
          storage::metric::ThreadLevelStatsCollector::GetCollectorForThread()->CollectTupleInsert(txn, database_oid,
                                                                                                  table_oid);
          insert_map[txn_id]++;
        } else {  // Delete
          storage::metric::ThreadLevelStatsCollector::GetCollectorForThread()->CollectTupleDelete(txn, database_oid,
                                                                                                  table_oid);
          delete_map[txn_id]++;
        }
      }
      txn_manager_->Commit(txn, TestCallbacks::EmptyCallback, nullptr);
      auto latency = static_cast<uint64_t>(
          std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now() - start)
              .count());
      latency_map[txn_id] = latency;
    }

    auto result = aggregator.AggregateRawData();
    EXPECT_FALSE(result.empty());

    for (auto &raw_data : result) {
      if (raw_data->GetMetricType() == storage::metric::MetricType::TRANSACTION) {
        for (uint8_t j = 0; j < num_txns_; j++) {
          auto txn_id = id_map[j];
          auto read_cnt =
              dynamic_cast<storage::metric::TransactionMetricRawData *>(raw_data.get())->GetTupleRead(txn_id);
          auto update_cnt =
              dynamic_cast<storage::metric::TransactionMetricRawData *>(raw_data.get())->GetTupleUpdate(txn_id);
          auto insert_cnt =
              dynamic_cast<storage::metric::TransactionMetricRawData *>(raw_data.get())->GetTupleInsert(txn_id);
          auto delete_cnt =
              dynamic_cast<storage::metric::TransactionMetricRawData *>(raw_data.get())->GetTupleDelete(txn_id);
          auto latency = dynamic_cast<storage::metric::TransactionMetricRawData *>(raw_data.get())->GetLatency(txn_id);

          EXPECT_EQ(read_cnt, read_map[txn_id]);
          EXPECT_EQ(update_cnt, update_map[txn_id]);
          EXPECT_EQ(insert_cnt, insert_map[txn_id]);
          EXPECT_EQ(delete_cnt, delete_map[txn_id]);
          EXPECT_GE(latency_map[txn_id], latency);
          EXPECT_LT(latency_map[txn_id], latency + acc_err);
        }
      }
    }
  }
}

TEST_F(MetricTests, TransactionMetricStorageTest) {
  auto stats_collector = storage::metric::ThreadLevelStatsCollector();
  storage::metric::StatsAggregator aggregator(txn_manager_, catalog_);

  catalog::table_oid_t table_oid = static_cast<catalog::table_oid_t>(2);  // any value
  const catalog::db_oid_t database_oid(catalog::DEFAULT_DATABASE_OID);

  for (uint8_t i = 0; i < num_iterations_; i++) {
    std::unordered_map<uint8_t, transaction::timestamp_t> id_map;
    std::unordered_map<transaction::timestamp_t, int32_t> read_map;
    std::unordered_map<transaction::timestamp_t, int32_t> update_map;
    std::unordered_map<transaction::timestamp_t, int32_t> insert_map;
    std::unordered_map<transaction::timestamp_t, int32_t> delete_map;
    std::unordered_map<transaction::timestamp_t, int64_t> latency_map;
    for (uint8_t j = 0; j < num_txns_; j++) {
      auto start = std::chrono::high_resolution_clock::now();
      auto *txn = txn_manager_->BeginTransaction();
      auto txn_id = txn->TxnId().load();
      id_map[j] = txn_id;
      read_map[txn_id] = 0;
      update_map[txn_id] = 0;
      insert_map[txn_id] = 0;
      delete_map[txn_id] = 0;

      auto num_ops_ = static_cast<uint8_t>(std::uniform_int_distribution<uint8_t>(1, UINT8_MAX)(generator_));
      for (uint8_t k = 0; k < num_ops_; k++) {
        auto op_type = std::uniform_int_distribution<uint8_t>(0, 3)(generator_);
        if (op_type == 0) {  // Read
          storage::metric::ThreadLevelStatsCollector::GetCollectorForThread()->CollectTupleRead(txn, database_oid,
                                                                                                table_oid);
          read_map[txn_id]++;
        } else if (op_type == 1) {  // Update
          storage::metric::ThreadLevelStatsCollector::GetCollectorForThread()->CollectTupleUpdate(txn, database_oid,
                                                                                                  table_oid);
          update_map[txn_id]++;
        } else if (op_type == 2) {  // Insert
          storage::metric::ThreadLevelStatsCollector::GetCollectorForThread()->CollectTupleInsert(txn, database_oid,
                                                                                                  table_oid);
          insert_map[txn_id]++;
        } else {  // Delete
          storage::metric::ThreadLevelStatsCollector::GetCollectorForThread()->CollectTupleDelete(txn, database_oid,
                                                                                                  table_oid);
          delete_map[txn_id]++;
        }
      }
      txn_manager_->Commit(txn, TestCallbacks::EmptyCallback, nullptr);
      auto latency = static_cast<uint64_t>(
          std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now() - start)
              .count());
      latency_map[txn_id] = latency;
    }

    aggregator.Aggregate(txn_);

    auto db_handle = catalog_->GetDatabaseHandle();
    auto table_handle = db_handle.GetNamespaceHandle(txn_, database_oid).GetTableHandle(txn_, "public");
    auto table = table_handle.GetTable(txn_, "txn_metric_table");

    for (uint8_t j = 0; j < num_txns_; j++) {
      auto txn_id = id_map[j];
      std::vector<type::TransientValue> search_vec;
      search_vec.emplace_back(type::TransientValueFactory::GetBigInt(static_cast<uint64_t>(txn_id)));
      auto row = table->FindRow(txn_, search_vec);
      auto latency = type::TransientValuePeeker::PeekBigInt(row[1]);
      auto read_cnt = type::TransientValuePeeker::PeekInteger(row[2]);
      auto insert_cnt = type::TransientValuePeeker::PeekInteger(row[3]);
      auto delete_cnt = type::TransientValuePeeker::PeekInteger(row[4]);
      auto update_cnt = type::TransientValuePeeker::PeekInteger(row[5]);
      EXPECT_EQ(read_cnt, read_map[txn_id]);
      EXPECT_EQ(update_cnt, update_map[txn_id]);
      EXPECT_EQ(insert_cnt, insert_map[txn_id]);
      EXPECT_EQ(delete_cnt, delete_map[txn_id]);
      EXPECT_GE(latency_map[txn_id], latency);
      EXPECT_LT(latency_map[txn_id], latency + acc_err);
    }
  }
}

}  // namespace terrier
