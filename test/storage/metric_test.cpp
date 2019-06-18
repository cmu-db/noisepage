#include <memory>
#include <random>
#include <string>
#include <thread>  //NOLINT
#include <unordered_map>
#include <vector>
#include "common/container/concurrent_map.h"
#include "metric/metrics_manager.h"
#include "metric/metrics_store.h"
#include "metric/transaction_metric.h"
#include "storage/garbage_collector.h"
#include "transaction/transaction_defs.h"
#include "transaction/transaction_manager.h"
#include "util/catalog_test_util.h"
#include "util/test_harness.h"
#include "util/transaction_test_util.h"

namespace terrier::metric {

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
    StartGC(txn_manager_);
    txn_ = txn_manager_->BeginTransaction();
  }

  void TearDown() override {
    txn_manager_->Commit(txn_, transaction::TransactionUtil::EmptyCallback, nullptr);
    EndGC();
    delete txn_manager_;
    TerrierTest::TearDown();
  }

  storage::RecordBufferSegmentPool buffer_pool_{100, 100};

  transaction::TransactionContext *txn_ = nullptr;
  transaction::TransactionManager *txn_manager_;
  std::default_random_engine generator_;
  const uint8_t num_iterations_ = 5;
  const uint8_t num_txns_ = 100;
  const std::chrono::milliseconds aggr_period_{1000};

  std::thread gc_thread_;
  storage::GarbageCollector *gc_ = nullptr;
  volatile bool run_gc_ = false;
  const std::chrono::milliseconds gc_period_{10};
};

/**
 * Basic test for testing transaction metric registration and stats collection, single thread
 */
// NOLINTNEXTLINE
TEST_F(MetricTests, TransactionMetricBasicTest) {
  for (uint8_t i = 0; i < num_iterations_; i++) {
    MetricsManager aggregator;

    const auto metrics_store_ptr = aggregator.RegisterThread();
    aggregator.EnableMetric(MetricsComponent::TRANSACTION);

    std::unordered_map<uint8_t, transaction::timestamp_t> id_map;
    std::unordered_map<transaction::timestamp_t, uint64_t> read_map;
    std::unordered_map<transaction::timestamp_t, uint64_t> update_map;
    std::unordered_map<transaction::timestamp_t, uint64_t> insert_map;
    std::unordered_map<transaction::timestamp_t, uint64_t> delete_map;
    std::unordered_map<transaction::timestamp_t, uint64_t> latency_min_map;
    std::unordered_map<transaction::timestamp_t, uint64_t> latency_max_map;

    for (uint8_t j = 0; j < num_txns_; j++) {
      auto start_max = std::chrono::high_resolution_clock::now();
      auto *txn = txn_manager_->BeginTransaction();
      metrics_store_ptr->RecordTransactionBegin(*txn);
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
          metrics_store_ptr->RecordTupleRead(*txn, CatalogTestUtil::test_db_oid, CatalogTestUtil::test_namespace_oid,
                                             CatalogTestUtil::test_table_oid);
          read_map[txn_id]++;
        } else if (op_type == 1) {  // Update
          metrics_store_ptr->RecordTupleUpdate(*txn, CatalogTestUtil::test_db_oid, CatalogTestUtil::test_namespace_oid,
                                               CatalogTestUtil::test_table_oid);
          update_map[txn_id]++;
        } else if (op_type == 2) {  // Insert
          metrics_store_ptr->RecordTupleInsert(*txn, CatalogTestUtil::test_db_oid, CatalogTestUtil::test_namespace_oid,
                                               CatalogTestUtil::test_table_oid);
          insert_map[txn_id]++;
        } else {  // Delete
          metrics_store_ptr->RecordTupleDelete(*txn, CatalogTestUtil::test_db_oid, CatalogTestUtil::test_namespace_oid,
                                               CatalogTestUtil::test_table_oid);
          delete_map[txn_id]++;
        }
      }
      auto latency = static_cast<uint64_t>(
          std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - start_min)
              .count());
      latency_min_map[txn_id] = latency;
      metrics_store_ptr->RecordTransactionCommit(*txn, CatalogTestUtil::test_db_oid);
      txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
      latency = static_cast<uint64_t>(
          std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - start_max)
              .count());
      latency_max_map[txn_id] = latency;
    }

    aggregator.Aggregate();
    const auto &result = aggregator.AggregatedMetrics();
    EXPECT_FALSE(result.empty());

    for (const auto &raw_data : result) {
      if (raw_data != nullptr && raw_data->GetMetricType() == MetricsComponent::TRANSACTION) {
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

    aggregator.UnregisterThread();
  }
}

/**
 *  Testing transaction metric stats collection and persistence, single thread
 */
// NOLINTNEXTLINE
TEST_F(MetricTests, TransactionMetricStorageTest) {
  for (uint8_t i = 0; i < num_iterations_; i++) {
    MetricsManager aggregator;

    const auto metrics_store_ptr = aggregator.RegisterThread();
    aggregator.EnableMetric(MetricsComponent::TRANSACTION);

    std::unordered_map<uint8_t, transaction::timestamp_t> id_map;
    std::unordered_map<transaction::timestamp_t, uint64_t> read_map;
    std::unordered_map<transaction::timestamp_t, uint64_t> update_map;
    std::unordered_map<transaction::timestamp_t, uint64_t> insert_map;
    std::unordered_map<transaction::timestamp_t, uint64_t> delete_map;
    std::unordered_map<transaction::timestamp_t, uint64_t> latency_min_map;
    std::unordered_map<transaction::timestamp_t, uint64_t> latency_max_map;
    for (uint8_t j = 0; j < num_txns_; j++) {
      auto start_max = std::chrono::high_resolution_clock::now();
      auto *txn = txn_manager_->BeginTransaction();
      metrics_store_ptr->RecordTransactionBegin(*txn);
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
          metrics_store_ptr->RecordTupleRead(*txn, CatalogTestUtil::test_db_oid, CatalogTestUtil::test_namespace_oid,
                                             CatalogTestUtil::test_table_oid);
          read_map[txn_id]++;
        } else if (op_type == 1) {  // Update
          metrics_store_ptr->RecordTupleUpdate(*txn, CatalogTestUtil::test_db_oid, CatalogTestUtil::test_namespace_oid,
                                               CatalogTestUtil::test_table_oid);
          update_map[txn_id]++;
        } else if (op_type == 2) {  // Insert
          metrics_store_ptr->RecordTupleInsert(*txn, CatalogTestUtil::test_db_oid, CatalogTestUtil::test_namespace_oid,
                                               CatalogTestUtil::test_table_oid);
          insert_map[txn_id]++;
        } else {  // Delete
          metrics_store_ptr->RecordTupleDelete(*txn, CatalogTestUtil::test_db_oid, CatalogTestUtil::test_namespace_oid,
                                               CatalogTestUtil::test_table_oid);
          delete_map[txn_id]++;
        }
      }
      auto latency = static_cast<uint64_t>(
          std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - start_min)
              .count());
      latency_min_map[txn_id] = latency;
      metrics_store_ptr->RecordTransactionCommit(*txn, CatalogTestUtil::test_db_oid);
      txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
      latency = static_cast<uint64_t>(
          std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - start_max)
              .count());
      latency_max_map[txn_id] = latency;
    }

    aggregator.Aggregate();

    aggregator.UnregisterThread();
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
    common::ConcurrentMap<transaction::timestamp_t, uint64_t> latency_max_map;
    common::ConcurrentMap<transaction::timestamp_t, uint64_t> latency_min_map;
    MetricsManager aggregator;
    aggregator.EnableMetric(MetricsComponent::TRANSACTION);
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
        const auto metrics_store_ptr = aggregator.RegisterThread();
        for (uint8_t j = 0; j < num_txns_; j++) {
          auto start_max = std::chrono::high_resolution_clock::now();
          auto *txn = txn_manager_->BeginTransaction();
          metrics_store_ptr->RecordTransactionBegin(*txn);
          auto start_min = std::chrono::high_resolution_clock::now();
          auto txn_id = txn->TxnId().load();
          txn_queue.Enqueue(txn_id);
          for (uint8_t k = 0; k < num_read; k++) {
            metrics_store_ptr->RecordTupleRead(*txn, CatalogTestUtil::test_db_oid, CatalogTestUtil::test_namespace_oid,
                                               CatalogTestUtil::test_table_oid);
          }
          for (uint8_t k = 0; k < num_update; k++) {
            metrics_store_ptr->RecordTupleUpdate(*txn, CatalogTestUtil::test_db_oid,
                                                 CatalogTestUtil::test_namespace_oid, CatalogTestUtil::test_table_oid);
          }
          for (uint8_t k = 0; k < num_insert; k++) {
            metrics_store_ptr->RecordTupleInsert(*txn, CatalogTestUtil::test_db_oid,
                                                 CatalogTestUtil::test_namespace_oid, CatalogTestUtil::test_table_oid);
          }
          for (uint8_t k = 0; k < num_delete; k++) {
            metrics_store_ptr->RecordTupleDelete(*txn, CatalogTestUtil::test_db_oid,
                                                 CatalogTestUtil::test_namespace_oid, CatalogTestUtil::test_table_oid);
          }
          auto latency = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(
                                                   std::chrono::high_resolution_clock::now() - start_min)
                                                   .count());
          latency_min_map.Insert(txn_id, latency);
          metrics_store_ptr->RecordTransactionCommit(*txn, CatalogTestUtil::test_db_oid);
          txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
          latency = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(
                                              std::chrono::high_resolution_clock::now() - start_max)
                                              .count());
          latency_max_map.Insert(txn_id, latency);
        }
      }
    };
    MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, num_threads, workload);
  }
}
}  // namespace terrier::metric
