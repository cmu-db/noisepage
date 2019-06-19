#include <memory>
#include <random>
#include <string>
#include <thread>  //NOLINT
#include <unordered_map>
#include <utility>
#include <vector>
#include "common/container/concurrent_map.h"
#include "main/db_main.h"
#include "metrics/metrics_manager.h"
#include "metrics/metrics_store.h"
#include "metrics/transaction_metric.h"
#include "settings/settings_callbacks.h"
#include "settings/settings_manager.h"
#include "storage/garbage_collector.h"
#include "transaction/transaction_defs.h"
#include "transaction/transaction_manager.h"
#include "util/catalog_test_util.h"
#include "util/test_harness.h"
#include "util/transaction_test_util.h"

#define __SETTING_GFLAGS_DEFINE__      // NOLINT
#include "settings/settings_common.h"  // NOLINT
#include "settings/settings_defs.h"    // NOLINT
#undef __SETTING_GFLAGS_DEFINE__       // NOLINT

namespace terrier::metrics {

/**
 * @brief Test the correctness of database metric
 */
class MetricsTests : public TerrierTest {
 public:
  DBMain *db_main_;
  settings::SettingsManager *settings_manager_;
  MetricsManager *metrics_manager_;
  transaction::TransactionManager *txn_manager_;

  void SetUp() override {
    std::unordered_map<settings::Param, settings::ParamInfo> param_map;
    terrier::settings::SettingsManager::ConstructParamMap(param_map);

    db_main_ = new DBMain(std::move(param_map));
    settings_manager_ = db_main_->settings_manager_;
    metrics_manager_ = db_main_->metrics_manager_;
    txn_manager_ = db_main_->txn_manager_;
  }

  void TearDown() override { delete db_main_; }

  std::default_random_engine generator_;
  const uint8_t num_iterations_ = 5;
  const uint8_t num_txns_ = 100;

  static void EmptySetterCallback(const std::shared_ptr<common::ActionContext> &action_context UNUSED_ATTRIBUTE) {}
};

/**
 * Basic test for testing transaction metric registration and stats collection, single thread
 */
// NOLINTNEXTLINE
TEST_F(MetricsTests, TransactionMetricBasicTest) {
  for (uint8_t i = 0; i < num_iterations_; i++) {
    const settings::setter_callback_fn setter_callback = MetricsTests::EmptySetterCallback;
    std::shared_ptr<common::ActionContext> action_context =
        std::make_shared<common::ActionContext>(common::action_id_t(1));
    settings_manager_->SetBool(settings::Param::metrics_transaction, true, action_context, setter_callback);

    const auto metrics_store_ptr = metrics_manager_->RegisterThread();

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
      auto txn_start = txn->StartTime();
      id_map[j] = txn_start;
      read_map[txn_start] = 0;
      update_map[txn_start] = 0;
      insert_map[txn_start] = 0;
      delete_map[txn_start] = 0;

      auto num_ops_ = static_cast<uint8_t>(std::uniform_int_distribution<uint8_t>(1, UINT8_MAX)(generator_));
      for (uint8_t k = 0; k < num_ops_; k++) {
        auto op_type = std::uniform_int_distribution<uint8_t>(0, 3)(generator_);
        if (op_type == 0) {  // Read
          metrics_store_ptr->RecordTupleRead(*txn, CatalogTestUtil::test_db_oid, CatalogTestUtil::test_namespace_oid,
                                             CatalogTestUtil::test_table_oid);
          read_map[txn_start]++;
        } else if (op_type == 1) {  // Update
          metrics_store_ptr->RecordTupleUpdate(*txn, CatalogTestUtil::test_db_oid, CatalogTestUtil::test_namespace_oid,
                                               CatalogTestUtil::test_table_oid);
          update_map[txn_start]++;
        } else if (op_type == 2) {  // Insert
          metrics_store_ptr->RecordTupleInsert(*txn, CatalogTestUtil::test_db_oid, CatalogTestUtil::test_namespace_oid,
                                               CatalogTestUtil::test_table_oid);
          insert_map[txn_start]++;
        } else {  // Delete
          metrics_store_ptr->RecordTupleDelete(*txn, CatalogTestUtil::test_db_oid, CatalogTestUtil::test_namespace_oid,
                                               CatalogTestUtil::test_table_oid);
          delete_map[txn_start]++;
        }
      }
      auto latency = static_cast<uint64_t>(
          std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - start_min)
              .count());
      latency_min_map[txn_start] = latency;
      metrics_store_ptr->RecordTransactionCommit(*txn, CatalogTestUtil::test_db_oid);
      txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
      latency = static_cast<uint64_t>(
          std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - start_max)
              .count());
      latency_max_map[txn_start] = latency;
    }

    metrics_manager_->Aggregate();
    const auto &result = metrics_manager_->AggregatedMetrics();
    EXPECT_FALSE(result.empty());

    for (const auto &raw_data : result) {
      if (raw_data != nullptr && raw_data->GetMetricType() == MetricsComponent::TRANSACTION) {
        for (uint8_t j = 0; j < num_txns_; j++) {
          auto txn_start = id_map[j];
          auto read_cnt = dynamic_cast<TransactionMetricRawData *>(raw_data.get())->GetTupleRead(txn_start);
          auto update_cnt = dynamic_cast<TransactionMetricRawData *>(raw_data.get())->GetTupleUpdate(txn_start);
          auto insert_cnt = dynamic_cast<TransactionMetricRawData *>(raw_data.get())->GetTupleInsert(txn_start);
          auto delete_cnt = dynamic_cast<TransactionMetricRawData *>(raw_data.get())->GetTupleDelete(txn_start);
          auto latency = dynamic_cast<TransactionMetricRawData *>(raw_data.get())->GetLatency(txn_start);

          EXPECT_EQ(read_cnt, read_map[txn_start]);
          EXPECT_EQ(update_cnt, update_map[txn_start]);
          EXPECT_EQ(insert_cnt, insert_map[txn_start]);
          EXPECT_EQ(delete_cnt, delete_map[txn_start]);
          EXPECT_GE(latency_max_map[txn_start], latency);
          EXPECT_LE(latency_min_map[txn_start], latency);
        }
      }
    }

    action_context = std::make_shared<common::ActionContext>(common::action_id_t(1));
    settings_manager_->SetBool(settings::Param::metrics_transaction, false, action_context, setter_callback);
    metrics_manager_->UnregisterThread();
  }
}

/**
 *  Testing transaction metric stats collection and persistence, single thread
 */
// NOLINTNEXTLINE
TEST_F(MetricsTests, TransactionMetricStorageTest) {
  for (uint8_t i = 0; i < num_iterations_; i++) {
    const settings::setter_callback_fn setter_callback = MetricsTests::EmptySetterCallback;
    std::shared_ptr<common::ActionContext> action_context =
        std::make_shared<common::ActionContext>(common::action_id_t(1));
    settings_manager_->SetBool(settings::Param::metrics_transaction, true, action_context, setter_callback);
    const auto metrics_store_ptr = metrics_manager_->RegisterThread();

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
      auto txn_start = txn->StartTime();
      id_map[j] = txn_start;
      read_map[txn_start] = 0;
      update_map[txn_start] = 0;
      insert_map[txn_start] = 0;
      delete_map[txn_start] = 0;

      auto num_ops_ = static_cast<uint8_t>(std::uniform_int_distribution<uint8_t>(1, UINT8_MAX)(generator_));
      for (uint8_t k = 0; k < num_ops_; k++) {
        auto op_type = std::uniform_int_distribution<uint8_t>(0, 3)(generator_);
        if (op_type == 0) {  // Read
          metrics_store_ptr->RecordTupleRead(*txn, CatalogTestUtil::test_db_oid, CatalogTestUtil::test_namespace_oid,
                                             CatalogTestUtil::test_table_oid);
          read_map[txn_start]++;
        } else if (op_type == 1) {  // Update
          metrics_store_ptr->RecordTupleUpdate(*txn, CatalogTestUtil::test_db_oid, CatalogTestUtil::test_namespace_oid,
                                               CatalogTestUtil::test_table_oid);
          update_map[txn_start]++;
        } else if (op_type == 2) {  // Insert
          metrics_store_ptr->RecordTupleInsert(*txn, CatalogTestUtil::test_db_oid, CatalogTestUtil::test_namespace_oid,
                                               CatalogTestUtil::test_table_oid);
          insert_map[txn_start]++;
        } else {  // Delete
          metrics_store_ptr->RecordTupleDelete(*txn, CatalogTestUtil::test_db_oid, CatalogTestUtil::test_namespace_oid,
                                               CatalogTestUtil::test_table_oid);
          delete_map[txn_start]++;
        }
      }
      auto latency = static_cast<uint64_t>(
          std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - start_min)
              .count());
      latency_min_map[txn_start] = latency;
      metrics_store_ptr->RecordTransactionCommit(*txn, CatalogTestUtil::test_db_oid);
      txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
      latency = static_cast<uint64_t>(
          std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - start_max)
              .count());
      latency_max_map[txn_start] = latency;
    }

    metrics_manager_->Aggregate();

    action_context = std::make_shared<common::ActionContext>(common::action_id_t(1));
    settings_manager_->SetBool(settings::Param::metrics_transaction, false, action_context, setter_callback);
    metrics_manager_->UnregisterThread();
  }
}

/**
 *  Testing metric stats collection and persistence, multiple threads
 */
// NOLINTNEXTLINE
TEST_F(MetricsTests, MultiThreadTest) {
  const uint32_t num_threads = MultiThreadTestUtil::HardwareConcurrency();
  common::WorkerPool thread_pool(num_threads, {});

  for (uint8_t i = 0; i < num_iterations_; i++) {
    common::ConcurrentQueue<transaction::timestamp_t> txn_queue;
    common::ConcurrentMap<transaction::timestamp_t, uint64_t> latency_max_map;
    common::ConcurrentMap<transaction::timestamp_t, uint64_t> latency_min_map;

    const settings::setter_callback_fn setter_callback = MetricsTests::EmptySetterCallback;
    std::shared_ptr<common::ActionContext> action_context =
        std::make_shared<common::ActionContext>(common::action_id_t(1));
    settings_manager_->SetBool(settings::Param::metrics_transaction, true, action_context, setter_callback);

    auto num_read = static_cast<uint8_t>(std::uniform_int_distribution<uint8_t>(1, UINT8_MAX)(generator_));
    auto num_update = static_cast<uint8_t>(std::uniform_int_distribution<uint8_t>(1, UINT8_MAX)(generator_));
    auto num_insert = static_cast<uint8_t>(std::uniform_int_distribution<uint8_t>(1, UINT8_MAX)(generator_));
    auto num_delete = static_cast<uint8_t>(std::uniform_int_distribution<uint8_t>(1, UINT8_MAX)(generator_));

    auto workload = [&](uint32_t id) {
      // NOTICE: thread level collector must be alive while aggregating
      if (id == 0) {  // aggregator thread
        std::this_thread::sleep_for(std::chrono::seconds(2));
        metrics_manager_->Aggregate();
      } else {  // normal thread
        const auto metrics_store_ptr = metrics_manager_->RegisterThread();
        for (uint8_t j = 0; j < num_txns_; j++) {
          auto start_max = std::chrono::high_resolution_clock::now();
          auto *txn = txn_manager_->BeginTransaction();
          metrics_store_ptr->RecordTransactionBegin(*txn);
          auto start_min = std::chrono::high_resolution_clock::now();
          auto txn_start = txn->StartTime();
          txn_queue.Enqueue(txn_start);
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
          latency_min_map.Insert(txn_start, latency);
          metrics_store_ptr->RecordTransactionCommit(*txn, CatalogTestUtil::test_db_oid);
          txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
          latency = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(
                                              std::chrono::high_resolution_clock::now() - start_max)
                                              .count());
          latency_max_map.Insert(txn_start, latency);
        }
        std::this_thread::sleep_for(std::chrono::seconds(4));
        metrics_manager_->UnregisterThread();
      }
    };
    MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, num_threads, workload);

    action_context = std::make_shared<common::ActionContext>(common::action_id_t(1));
    settings_manager_->SetBool(settings::Param::metrics_transaction, false, action_context, setter_callback);
  }
}
}  // namespace terrier::metrics
