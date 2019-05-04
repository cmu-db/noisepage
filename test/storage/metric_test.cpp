#include <memory>
#include <random>
#include <thread>  //NOLINT
#include <unordered_map>
#include <vector>
#include "storage/garbage_collector.h"
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

namespace settings {
class SettingsManager;
}

/**
 * @brief Test the correctness of database metric
 */
class MetricTests : public TerrierTest {
 public:
  void GCThreadLoop() {
    while (run_gc_) {
      std::this_thread::sleep_for(gc_period_);
      gc_->PerformGarbageCollection();
    }
  }

  void StartGC(transaction::TransactionManager *const txn_manager) {
    gc_ = new storage::GarbageCollector(txn_manager);
    run_gc_ = true;
    gc_thread_ = std::thread([this] { GCThreadLoop(); });
  }

  void EndGC() {
    run_gc_ = false;
    gc_thread_.join();
    // Make sure all garbage is collected. This take 2 runs for unlink and deallocate
    gc_->PerformGarbageCollection();
    gc_->PerformGarbageCollection();
    delete gc_;
  }

  void SetUp() override {
    TerrierTest::SetUp();
    txn_manager_ = new transaction::TransactionManager(&buffer_pool_, true, LOGGING_DISABLED);

    txn_ = txn_manager_->BeginTransaction();
    catalog_ = new catalog::Catalog(txn_manager_, txn_);
    settings_manager_ = nullptr;
    StartGC(txn_manager_);
  }

  void TearDown() override {
    txn_manager_->Commit(txn_, TestCallbacks::EmptyCallback, nullptr);
    TerrierTest::TearDown();
    EndGC();
    const catalog::db_oid_t terrier_oid(catalog::DEFAULT_DATABASE_OID);
    auto db_handle = catalog_->GetDatabaseHandle();
    auto table_handle = db_handle.GetNamespaceHandle(txn_, terrier_oid).GetTableHandle(txn_, "public");
    auto table = table_handle.GetTable(txn_, "database_metric_table");
    delete table;
    db_handle = catalog_->GetDatabaseHandle();
    table_handle = db_handle.GetNamespaceHandle(txn_, terrier_oid).GetTableHandle(txn_, "public");
    table = table_handle.GetTable(txn_, "txn_metric_table");
    delete table;
    delete catalog_;  // need to delete catalog_first
    delete txn_manager_;
  }
  settings::SettingsManager *settings_manager_;

  catalog::Catalog *catalog_;
  storage::RecordBufferSegmentPool buffer_pool_{100, 100};

  transaction::TransactionContext *txn_ = nullptr;
  transaction::TransactionManager *txn_manager_;
  std::default_random_engine generator_;
  const uint8_t num_iterations_ = 2;
  const uint8_t num_databases_ = 2;
  const uint8_t num_txns_ = 2;
  const int64_t acc_err = 5;

  std::thread gc_thread_;
  storage::GarbageCollector *gc_ = nullptr;
  volatile bool run_gc_ = false;
  const std::chrono::milliseconds gc_period_{10};
};

/**
 * Basic test for testing metric registration and stats collection
 */
// NOLINTNEXTLINE
TEST_F(MetricTests, BasicTest) {
  storage::metric::StatsAggregator aggregator(txn_manager_, catalog_, nullptr);
  EXPECT_EQ(aggregator.GetTxnManager(), txn_manager_);
  EXPECT_EQ(aggregator.GetCatalog(), catalog_);
  EXPECT_EQ(aggregator.GetSettingsManager(), settings_manager_);

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
  storage::metric::StatsAggregator aggregator(txn_manager_, catalog_, nullptr);
  for (uint8_t i = 0; i < num_iterations_; i++) {
    std::unordered_map<uint8_t, int64_t> commit_map;
    std::unordered_map<uint8_t, int64_t> abort_map;
    for (uint8_t j = 0; j < num_databases_; j++) {
      commit_map[j] = 0;
      abort_map[j] = 0;
      auto num_txns_ = static_cast<uint8_t>(std::uniform_int_distribution<uint8_t>(0, UINT8_MAX)(generator_));
      for (uint8_t k = 0; k < num_txns_; k++) {
        auto *txn = txn_manager_->BeginTransaction();
        storage::metric::ThreadLevelStatsCollector::GetCollectorForThread()->CollectTransactionBegin(txn);
        auto txn_res = static_cast<bool>(std::uniform_int_distribution<uint8_t>(0, 1)(generator_));
        if (txn_res) {
          storage::metric::ThreadLevelStatsCollector::GetCollectorForThread()->CollectTransactionCommit(
              txn, static_cast<catalog::db_oid_t>(j));
          txn_manager_->Commit(txn, TestCallbacks::EmptyCallback, nullptr);
          commit_map[j]++;
        } else {
          storage::metric::ThreadLevelStatsCollector::GetCollectorForThread()->CollectTransactionAbort(
              txn, static_cast<catalog::db_oid_t>(j));
          txn_manager_->Abort(txn);
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
  storage::metric::StatsAggregator aggregator(txn_manager_, catalog_, nullptr);
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
          storage::metric::ThreadLevelStatsCollector::GetCollectorForThread()->CollectTransactionCommit(
              txn, static_cast<catalog::db_oid_t>(j));
          txn_manager_->Commit(txn, TestCallbacks::EmptyCallback, nullptr);
          commit_map[j]++;
        } else {
          storage::metric::ThreadLevelStatsCollector::GetCollectorForThread()->CollectTransactionAbort(
              txn, static_cast<catalog::db_oid_t>(j));
          txn_manager_->Abort(txn);
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
      auto commit_cnt = type::TransientValuePeeker::PeekBigInt(row[1]);
      auto abort_cnt = type::TransientValuePeeker::PeekBigInt(row[2]);
      EXPECT_EQ(commit_cnt, commit_map[j]);
      EXPECT_EQ(abort_cnt, abort_map[j]);
    }
  }
}

/**
 * Basic test for testing transaction metric registration and stats collection, single thread
 */
// NOLINTNEXTLINE
TEST_F(MetricTests, TransactionMetricBasicTest) {
  auto stats_collector = storage::metric::ThreadLevelStatsCollector();
  storage::metric::StatsAggregator aggregator(txn_manager_, catalog_, nullptr);

  catalog::table_oid_t table_oid = static_cast<catalog::table_oid_t>(2);  // any value
  const catalog::db_oid_t database_oid(catalog::DEFAULT_DATABASE_OID);

  for (uint8_t i = 0; i < num_iterations_; i++) {
    std::unordered_map<uint8_t, transaction::timestamp_t> id_map;
    std::unordered_map<transaction::timestamp_t, int64_t> read_map;
    std::unordered_map<transaction::timestamp_t, int64_t> update_map;
    std::unordered_map<transaction::timestamp_t, int64_t> insert_map;
    std::unordered_map<transaction::timestamp_t, int64_t> delete_map;
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
      auto latency = static_cast<int64_t>(
          std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now() - start)
              .count());
      txn_manager_->Commit(txn, TestCallbacks::EmptyCallback, nullptr);
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

/**
 *  Testing transaction metric stats collection and persistence, single thread
 */
// NOLINTNEXTLINE
TEST_F(MetricTests, TransactionMetricStorageTest) {
  auto stats_collector = storage::metric::ThreadLevelStatsCollector();
  storage::metric::StatsAggregator aggregator(txn_manager_, catalog_, nullptr);

  catalog::table_oid_t table_oid = static_cast<catalog::table_oid_t>(2);  // any value
  const catalog::db_oid_t database_oid(catalog::DEFAULT_DATABASE_OID);

  for (uint8_t i = 0; i < num_iterations_; i++) {
    std::unordered_map<uint8_t, transaction::timestamp_t> id_map;
    std::unordered_map<transaction::timestamp_t, int64_t> read_map;
    std::unordered_map<transaction::timestamp_t, int64_t> update_map;
    std::unordered_map<transaction::timestamp_t, int64_t> insert_map;
    std::unordered_map<transaction::timestamp_t, int64_t> delete_map;
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
      auto latency = static_cast<uint64_t>(
          std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now() - start)
              .count());
      txn_manager_->Commit(txn, TestCallbacks::EmptyCallback, nullptr);
      latency_map[txn_id] = latency;
    }

    aggregator.Aggregate(txn_);

    auto db_handle = catalog_->GetDatabaseHandle();
    auto table_handle = db_handle.GetNamespaceHandle(txn_, database_oid).GetTableHandle(txn_, "public");
    auto table = table_handle.GetTable(txn_, "txn_metric_table");

    for (uint8_t j = 0; j < num_txns_; j++) {
      auto txn_id = id_map[j];
      std::vector<type::TransientValue> search_vec;
      search_vec.emplace_back(
          type::TransientValueFactory::GetBigInt(static_cast<int64_t>(static_cast<uint64_t>(txn_id))));
      auto row = table->FindRow(txn_, search_vec);
      auto latency = type::TransientValuePeeker::PeekBigInt(row[1]);
      auto read_cnt = type::TransientValuePeeker::PeekBigInt(row[2]);
      auto insert_cnt = type::TransientValuePeeker::PeekBigInt(row[3]);
      auto delete_cnt = type::TransientValuePeeker::PeekBigInt(row[4]);
      auto update_cnt = type::TransientValuePeeker::PeekBigInt(row[5]);
      EXPECT_EQ(read_cnt, read_map[txn_id]);
      EXPECT_EQ(update_cnt, update_map[txn_id]);
      EXPECT_EQ(insert_cnt, insert_map[txn_id]);
      EXPECT_EQ(delete_cnt, delete_map[txn_id]);
      EXPECT_GE(latency_map[txn_id], latency);
      EXPECT_LT(latency_map[txn_id], latency + acc_err);
    }
  }
}

/**
 *  Testing metric stats collection and persistence, multiple threads
 */
// NOLINTNEXTLINE
TEST_F(MetricTests, MultiThreadTest) {
  const uint32_t num_threads = MultiThreadTestUtil::HardwareConcurrency();
  common::WorkerPool thread_pool(num_threads, {});

  catalog::table_oid_t table_oid = static_cast<catalog::table_oid_t>(2);  // any value
  const catalog::db_oid_t database_oid(catalog::DEFAULT_DATABASE_OID);

  for (uint8_t i = 0; i < num_iterations_; i++) {
    common::ConcurrentQueue<transaction::timestamp_t> txn_queue;
    common::ConcurrentMap<transaction::timestamp_t, int64_t> latency_map;
    storage::metric::StatsAggregator aggregator(txn_manager_, catalog_, nullptr);
    common::ConcurrentVector<std::shared_ptr<storage::metric::ThreadLevelStatsCollector *>> collectors;
    auto num_read = static_cast<uint8_t>(std::uniform_int_distribution<uint8_t>(1, UINT8_MAX)(generator_));
    auto num_update = static_cast<uint8_t>(std::uniform_int_distribution<uint8_t>(1, UINT8_MAX)(generator_));
    auto num_insert = static_cast<uint8_t>(std::uniform_int_distribution<uint8_t>(1, UINT8_MAX)(generator_));
    auto num_delete = static_cast<uint8_t>(std::uniform_int_distribution<uint8_t>(1, UINT8_MAX)(generator_));

    auto workload = [&](uint32_t id) {
      // NOTICE: thread level collector must be alive while aggregating
      if (id == 0) {  // aggregator thread
        std::this_thread::sleep_for(std::chrono::milliseconds(3000));
        aggregator.Aggregate(txn_);
        auto iter = collectors.Begin();
        for (; iter != collectors.End(); iter++) {
          delete iter->get();
        }
      } else {  // normal thread
        auto *stats_collector = new storage::metric::ThreadLevelStatsCollector();
        collectors.PushBack(std::make_shared<storage::metric::ThreadLevelStatsCollector *>(stats_collector));
        for (uint8_t j = 0; j < num_txns_; j++) {
          auto start = std::chrono::high_resolution_clock::now();
          auto *txn = txn_manager_->BeginTransaction();
          auto txn_id = txn->TxnId().load();
          txn_queue.Enqueue(txn_id);
          for (uint8_t k = 0; k < num_read; k++) {
            storage::metric::ThreadLevelStatsCollector::GetCollectorForThread()->CollectTupleRead(txn, database_oid,
                                                                                                  table_oid);
          }
          for (uint8_t k = 0; k < num_update; k++) {
            storage::metric::ThreadLevelStatsCollector::GetCollectorForThread()->CollectTupleUpdate(txn, database_oid,
                                                                                                    table_oid);
          }
          for (uint8_t k = 0; k < num_insert; k++) {
            storage::metric::ThreadLevelStatsCollector::GetCollectorForThread()->CollectTupleInsert(txn, database_oid,
                                                                                                    table_oid);
          }
          for (uint8_t k = 0; k < num_delete; k++) {
            storage::metric::ThreadLevelStatsCollector::GetCollectorForThread()->CollectTupleDelete(txn, database_oid,
                                                                                                    table_oid);
          }
          auto latency = static_cast<uint64_t>(
              std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now() - start)
                  .count());
          txn_manager_->Commit(txn, TestCallbacks::EmptyCallback, nullptr);
          latency_map.Insert(txn_id, latency);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
      }
    };
    MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, num_threads, workload);

    auto db_handle = catalog_->GetDatabaseHandle();
    auto table_handle = db_handle.GetNamespaceHandle(txn_, database_oid).GetTableHandle(txn_, "public");
    auto table = table_handle.GetTable(txn_, "txn_metric_table");

    while (!txn_queue.Empty()) {
      transaction::timestamp_t txn_id;
      auto res = txn_queue.Dequeue(&txn_id);
      if (!res) break;
      std::vector<type::TransientValue> search_vec;
      search_vec.emplace_back(
          type::TransientValueFactory::GetBigInt(static_cast<int64_t>(static_cast<uint64_t>(txn_id))));
      auto row = table->FindRow(txn_, search_vec);
      auto latency = type::TransientValuePeeker::PeekBigInt(row[1]);
      auto read_cnt = type::TransientValuePeeker::PeekBigInt(row[2]);
      auto insert_cnt = type::TransientValuePeeker::PeekBigInt(row[3]);
      auto delete_cnt = type::TransientValuePeeker::PeekBigInt(row[4]);
      auto update_cnt = type::TransientValuePeeker::PeekBigInt(row[5]);
      EXPECT_EQ(read_cnt, num_read);
      EXPECT_EQ(update_cnt, num_update);
      EXPECT_EQ(insert_cnt, num_insert);
      EXPECT_EQ(delete_cnt, num_delete);
      EXPECT_GE(latency_map.Find(txn_id)->second, latency);
      EXPECT_LE(latency_map.Find(txn_id)->second, latency + acc_err);
    }
  }
}
}  // namespace terrier
