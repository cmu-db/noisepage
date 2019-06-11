#include <util/catalog_test_util.h>
#include <memory>
#include <random>
#include <string>
#include <thread>  //NOLINT
#include <unordered_map>
#include <vector>
#include "metric/database_metric.h"
#include "metric/stats_aggregator.h"
#include "metric/thread_level_stats_collector.h"
#include "metric/transaction_metric.h"
#include "storage/garbage_collector.h"
#include "transaction/transaction_defs.h"
#include "transaction/transaction_manager.h"
#include "util/metric_test_util.h"
#include "util/test_harness.h"
#include "util/transaction_test_util.h"

namespace terrier::metric {

/**
 * @brief Test the correctness of database metric
 */
class MetricTests : public TerrierTest {
 public:
  void SetUp() override {
    TerrierTest::SetUp();
    txn_manager_ = new transaction::TransactionManager(&buffer_pool_, true, LOGGING_DISABLED);
    txn_ = txn_manager_->BeginTransaction();
  }

  void TearDown() override {
    txn_manager_->Commit(txn_, transaction::TransactionUtil::EmptyCallback, nullptr);
    delete txn_manager_;
    TerrierTest::TearDown();
  }

  storage::RecordBufferSegmentPool buffer_pool_{100, 100};

  transaction::TransactionContext *txn_ = nullptr;
  transaction::TransactionManager *txn_manager_;
  std::default_random_engine generator_;
  const uint8_t num_iterations_ = 5;
  const uint8_t num_databases_ = 5;
  const uint8_t num_txns_ = 100;
  const std::chrono::milliseconds aggr_period_{1000};
};

/**
 * Basic test for testing metric registration and stats collection
 */
// NOLINTNEXTLINE
TEST_F(MetricTests, BasicTest) {
  auto test_num_1 = std::uniform_int_distribution<int32_t>(0, INT32_MAX)(generator_);
  auto test_num_2 = std::uniform_int_distribution<int32_t>(0, INT32_MAX)(generator_);
  auto stats_collector = ThreadLevelStatsCollector();
  ThreadLevelStatsCollector::GetCollectorForThread()->CollectTestNum(test_num_1);
  ThreadLevelStatsCollector::GetCollectorForThread()->CollectTestNum(test_num_2);

  EXPECT_EQ(TestingStatsUtil::AggregateTestCounts(), test_num_1 + test_num_2);
}

/**
 * Basic test for testing database metric registration and stats collection, single thread
 */
// NOLINTNEXTLINE
TEST_F(MetricTests, DatabaseMetricBasicTest) {
  const uint32_t num_threads = MultiThreadTestUtil::HardwareConcurrency();
  common::WorkerPool thread_pool(num_threads, {});
  auto stats_collector = ThreadLevelStatsCollector();
  StatsAggregator aggregator;
  for (uint8_t i = 0; i < num_iterations_; i++) {
    std::unordered_map<uint8_t, int64_t> commit_map;
    std::unordered_map<uint8_t, int64_t> abort_map;
    for (uint8_t j = 0; j < num_databases_; j++) {
      commit_map[j] = 0;
      abort_map[j] = 0;
      auto num_txns_ = static_cast<uint8_t>(std::uniform_int_distribution<uint8_t>(0, UINT8_MAX)(generator_));
      for (uint8_t k = 0; k < num_txns_; k++) {
        auto *txn = txn_manager_->BeginTransaction();
        ThreadLevelStatsCollector::GetCollectorForThread()->CollectTransactionBegin(txn);
        auto txn_res = static_cast<bool>(std::uniform_int_distribution<uint8_t>(0, 1)(generator_));
        if (txn_res) {
          ThreadLevelStatsCollector::GetCollectorForThread()->CollectTransactionCommit(
              txn, static_cast<catalog::db_oid_t>(j));
          txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
          commit_map[j]++;
        } else {
          ThreadLevelStatsCollector::GetCollectorForThread()->CollectTransactionAbort(
              txn, static_cast<catalog::db_oid_t>(j));
          txn_manager_->Abort(txn);
          abort_map[j]++;
        }
      }
    }

    auto result = aggregator.AggregateRawData();
    EXPECT_FALSE(result.empty());

    for (const auto &raw_data : result) {
      if (raw_data->GetMetricType() == MetricType::DATABASE) {
        for (uint8_t j = 0; j < num_databases_; j++) {
          auto commit_cnt =
              dynamic_cast<DatabaseMetricRawData *>(raw_data.get())->GetCommitCount(static_cast<catalog::db_oid_t>(j));
          auto abort_cnt =
              dynamic_cast<DatabaseMetricRawData *>(raw_data.get())->GetAbortCount(static_cast<catalog::db_oid_t>(j));
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
  auto stats_collector = ThreadLevelStatsCollector();
  StatsAggregator aggregator;
  std::unordered_map<uint8_t, int32_t> commit_map;
  std::unordered_map<uint8_t, int32_t> abort_map;
  for (uint8_t j = 0; j < num_databases_; j++) {
    commit_map[j] = 0;
    abort_map[j] = 0;
  }
  for (uint8_t i = 0; i < num_iterations_; i++) {
    //    auto stats_collector = ThreadLevelStatsCollector();
    for (uint8_t j = 0; j < num_databases_; j++) {
      auto num_txns_ = static_cast<uint8_t>(std::uniform_int_distribution<uint8_t>(1, UINT8_MAX)(generator_));
      for (uint8_t k = 0; k < num_txns_; k++) {
        auto *txn = txn_manager_->BeginTransaction();
        ThreadLevelStatsCollector::GetCollectorForThread()->CollectTransactionBegin(txn);
        auto txn_res = static_cast<bool>(std::uniform_int_distribution<uint8_t>(0, 1)(generator_));
        if (txn_res) {
          ThreadLevelStatsCollector::GetCollectorForThread()->CollectTransactionCommit(
              txn, static_cast<catalog::db_oid_t>(j));
          txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
          commit_map[j]++;
        } else {
          ThreadLevelStatsCollector::GetCollectorForThread()->CollectTransactionAbort(
              txn, static_cast<catalog::db_oid_t>(j));
          txn_manager_->Abort(txn);
          abort_map[j]++;
        }
      }
    }
    aggregator.Aggregate();
  }
}

/**
 * Basic test for testing transaction metric registration and stats collection, single thread
 */
// NOLINTNEXTLINE
TEST_F(MetricTests, TransactionMetricBasicTest) {
  auto stats_collector = ThreadLevelStatsCollector();
  StatsAggregator aggregator;

  for (uint8_t i = 0; i < num_iterations_; i++) {
    std::unordered_map<uint8_t, transaction::timestamp_t> id_map;
    std::unordered_map<transaction::timestamp_t, int64_t> read_map;
    std::unordered_map<transaction::timestamp_t, int64_t> update_map;
    std::unordered_map<transaction::timestamp_t, int64_t> insert_map;
    std::unordered_map<transaction::timestamp_t, int64_t> delete_map;
    std::unordered_map<transaction::timestamp_t, int64_t> latency_min_map;
    std::unordered_map<transaction::timestamp_t, int64_t> latency_max_map;
    for (uint8_t j = 0; j < num_txns_; j++) {
      auto start_max = std::chrono::high_resolution_clock::now();
      auto *txn = txn_manager_->BeginTransaction();
      ThreadLevelStatsCollector::GetCollectorForThread()->CollectTransactionBegin(txn);
      auto start_min = std::chrono::high_resolution_clock::now();
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
          ThreadLevelStatsCollector::GetCollectorForThread()->CollectTupleRead(
              txn, CatalogTestUtil::test_db_oid, CatalogTestUtil::test_namespace_oid, CatalogTestUtil::test_table_oid);
          read_map[txn_id]++;
        } else if (op_type == 1) {  // Update
          ThreadLevelStatsCollector::GetCollectorForThread()->CollectTupleUpdate(
              txn, CatalogTestUtil::test_db_oid, CatalogTestUtil::test_namespace_oid, CatalogTestUtil::test_table_oid);
          update_map[txn_id]++;
        } else if (op_type == 2) {  // Insert
          ThreadLevelStatsCollector::GetCollectorForThread()->CollectTupleInsert(
              txn, CatalogTestUtil::test_db_oid, CatalogTestUtil::test_namespace_oid, CatalogTestUtil::test_table_oid);
          insert_map[txn_id]++;
        } else {  // Delete
          ThreadLevelStatsCollector::GetCollectorForThread()->CollectTupleDelete(
              txn, CatalogTestUtil::test_db_oid, CatalogTestUtil::test_namespace_oid, CatalogTestUtil::test_table_oid);
          delete_map[txn_id]++;
        }
      }
      auto latency = static_cast<uint64_t>(
          std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - start_min)
              .count());
      latency_min_map[txn_id] = latency;
      ThreadLevelStatsCollector::GetCollectorForThread()->CollectTransactionCommit(txn, CatalogTestUtil::test_db_oid);
      txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
      latency = static_cast<uint64_t>(
          std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - start_max)
              .count());
      latency_max_map[txn_id] = latency;
    }

    auto result = aggregator.AggregateRawData();
    EXPECT_FALSE(result.empty());

    for (const auto &raw_data : result) {
      if (raw_data->GetMetricType() == MetricType::TRANSACTION) {
        for (uint8_t j = 0; j < num_txns_; j++) {
          auto txn_id = id_map[j];
          auto read_cnt = dynamic_cast<TransactionMetricRawData *>(raw_data.get())->GetTupleRead(txn_id);
          auto update_cnt = dynamic_cast<TransactionMetricRawData *>(raw_data.get())->GetTupleUpdate(txn_id);
          auto insert_cnt = dynamic_cast<TransactionMetricRawData *>(raw_data.get())->GetTupleInsert(txn_id);
          auto delete_cnt = dynamic_cast<TransactionMetricRawData *>(raw_data.get())->GetTupleDelete(txn_id);
          auto latency = dynamic_cast<TransactionMetricRawData *>(raw_data.get())->GetLatency(txn_id);

          EXPECT_EQ(read_cnt, read_map[txn_id]);
          EXPECT_EQ(update_cnt, update_map[txn_id]);
          EXPECT_EQ(insert_cnt, insert_map[txn_id]);
          EXPECT_EQ(delete_cnt, delete_map[txn_id]);
          EXPECT_GE(latency_max_map[txn_id], latency);
          EXPECT_LE(latency_min_map[txn_id], latency);
        }
      }
    }
  }
}

/**
 *  Testing transaction metric stats collection and persistence, single thread
 */
// NOLINTNEXTLINE
TEST_F(MetricTests, TransactionMetricStorageTest) {
  auto stats_collector = ThreadLevelStatsCollector();
  StatsAggregator aggregator;

  for (uint8_t i = 0; i < num_iterations_; i++) {
    std::unordered_map<uint8_t, transaction::timestamp_t> id_map;
    std::unordered_map<transaction::timestamp_t, int64_t> read_map;
    std::unordered_map<transaction::timestamp_t, int64_t> update_map;
    std::unordered_map<transaction::timestamp_t, int64_t> insert_map;
    std::unordered_map<transaction::timestamp_t, int64_t> delete_map;
    std::unordered_map<transaction::timestamp_t, int64_t> latency_min_map;
    std::unordered_map<transaction::timestamp_t, int64_t> latency_max_map;
    for (uint8_t j = 0; j < num_txns_; j++) {
      auto start_max = std::chrono::high_resolution_clock::now();
      auto *txn = txn_manager_->BeginTransaction();
      ThreadLevelStatsCollector::GetCollectorForThread()->CollectTransactionBegin(txn);
      auto start_min = std::chrono::high_resolution_clock::now();
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
          ThreadLevelStatsCollector::GetCollectorForThread()->CollectTupleRead(
              txn, CatalogTestUtil::test_db_oid, CatalogTestUtil::test_namespace_oid, CatalogTestUtil::test_table_oid);
          read_map[txn_id]++;
        } else if (op_type == 1) {  // Update
          ThreadLevelStatsCollector::GetCollectorForThread()->CollectTupleUpdate(
              txn, CatalogTestUtil::test_db_oid, CatalogTestUtil::test_namespace_oid, CatalogTestUtil::test_table_oid);
          update_map[txn_id]++;
        } else if (op_type == 2) {  // Insert
          ThreadLevelStatsCollector::GetCollectorForThread()->CollectTupleInsert(
              txn, CatalogTestUtil::test_db_oid, CatalogTestUtil::test_namespace_oid, CatalogTestUtil::test_table_oid);
          insert_map[txn_id]++;
        } else {  // Delete
          ThreadLevelStatsCollector::GetCollectorForThread()->CollectTupleDelete(
              txn, CatalogTestUtil::test_db_oid, CatalogTestUtil::test_namespace_oid, CatalogTestUtil::test_table_oid);
          delete_map[txn_id]++;
        }
      }
      auto latency = static_cast<uint64_t>(
          std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - start_min)
              .count());
      latency_min_map[txn_id] = latency;
      ThreadLevelStatsCollector::GetCollectorForThread()->CollectTransactionCommit(txn, CatalogTestUtil::test_db_oid);
      txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
      latency = static_cast<uint64_t>(
          std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - start_max)
              .count());
      latency_max_map[txn_id] = latency;
    }

    aggregator.Aggregate();
  }
}

/**
 *  Testing metric stats collection and persistence, multiple threads
 */
// NOLINTNEXTLINE
TEST_F(MetricTests, MultiThreadTest) {
  const uint32_t num_threads = MultiThreadTestUtil::HardwareConcurrency();
  common::WorkerPool thread_pool(num_threads, {});

  for (uint8_t i = 0; i < num_iterations_; i++) {
    common::ConcurrentQueue<transaction::timestamp_t> txn_queue;
    common::ConcurrentMap<transaction::timestamp_t, int64_t> latency_max_map;
    common::ConcurrentMap<transaction::timestamp_t, int64_t> latency_min_map;
    StatsAggregator aggregator;
    common::ConcurrentVector<ThreadLevelStatsCollector *> collectors;
    auto num_read = static_cast<uint8_t>(std::uniform_int_distribution<uint8_t>(1, UINT8_MAX)(generator_));
    auto num_update = static_cast<uint8_t>(std::uniform_int_distribution<uint8_t>(1, UINT8_MAX)(generator_));
    auto num_insert = static_cast<uint8_t>(std::uniform_int_distribution<uint8_t>(1, UINT8_MAX)(generator_));
    auto num_delete = static_cast<uint8_t>(std::uniform_int_distribution<uint8_t>(1, UINT8_MAX)(generator_));

    auto workload = [&](uint32_t id) {
      // NOTICE: thread level collector must be alive while aggregating
      if (id == 0) {  // aggregator thread
        std::this_thread::sleep_for(aggr_period_);
        aggregator.Aggregate();
      } else {  // normal thread
        auto *stats_collector = new ThreadLevelStatsCollector();
        collectors.PushBack(stats_collector);
        for (uint8_t j = 0; j < num_txns_; j++) {
          auto start_max = std::chrono::high_resolution_clock::now();
          auto *txn = txn_manager_->BeginTransaction();
          ThreadLevelStatsCollector::GetCollectorForThread()->CollectTransactionBegin(txn);
          auto start_min = std::chrono::high_resolution_clock::now();
          auto txn_id = txn->TxnId().load();
          txn_queue.Enqueue(txn_id);
          for (uint8_t k = 0; k < num_read; k++) {
            ThreadLevelStatsCollector::GetCollectorForThread()->CollectTupleRead(txn, CatalogTestUtil::test_db_oid,
                                                                                 CatalogTestUtil::test_namespace_oid,
                                                                                 CatalogTestUtil::test_table_oid);
          }
          for (uint8_t k = 0; k < num_update; k++) {
            ThreadLevelStatsCollector::GetCollectorForThread()->CollectTupleUpdate(txn, CatalogTestUtil::test_db_oid,
                                                                                   CatalogTestUtil::test_namespace_oid,
                                                                                   CatalogTestUtil::test_table_oid);
          }
          for (uint8_t k = 0; k < num_insert; k++) {
            ThreadLevelStatsCollector::GetCollectorForThread()->CollectTupleInsert(txn, CatalogTestUtil::test_db_oid,
                                                                                   CatalogTestUtil::test_namespace_oid,
                                                                                   CatalogTestUtil::test_table_oid);
          }
          for (uint8_t k = 0; k < num_delete; k++) {
            ThreadLevelStatsCollector::GetCollectorForThread()->CollectTupleDelete(txn, CatalogTestUtil::test_db_oid,
                                                                                   CatalogTestUtil::test_namespace_oid,
                                                                                   CatalogTestUtil::test_table_oid);
          }
          auto latency = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(
                                                   std::chrono::high_resolution_clock::now() - start_min)
                                                   .count());
          latency_min_map.Insert(txn_id, latency);
          ThreadLevelStatsCollector::GetCollectorForThread()->CollectTransactionCommit(txn,
                                                                                       CatalogTestUtil::test_db_oid);
          txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
          latency = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(
                                              std::chrono::high_resolution_clock::now() - start_max)
                                              .count());
          latency_max_map.Insert(txn_id, latency);
        }
      }
    };
    MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, num_threads, workload);
    for (auto iter = collectors.Begin(); iter != collectors.End(); iter++) {
      delete *iter;
    }
  }
}
}  // namespace terrier::metric
