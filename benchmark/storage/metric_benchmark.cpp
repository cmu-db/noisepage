#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include "benchmark/benchmark.h"
#include "common/strong_typedef.h"
#include "storage/garbage_collector.h"
#include "metric/database_metric.h"
#include "metric/stats_aggregator.h"
#include "metric/thread_level_stats_collector.h"
#include "metric/transaction_metric.h"
#include "transaction/transaction_defs.h"
#include "transaction/transaction_manager.h"
#include "util/metric_test_util.h"
#include "util/test_harness.h"
#include "util/transaction_test_util.h"

namespace terrier {

// This benchmark measures the throughput of the Aggregator and the Collector in the metric collection pipeline.
class MetricBenchmark : public benchmark::Fixture {
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

  void SetUp(const benchmark::State &state) final {
    txn_manager_ = new transaction::TransactionManager(&buffer_pool_, true, LOGGING_DISABLED);

    txn_ = txn_manager_->BeginTransaction();
    settings_manager_ = nullptr;
    StartGC(txn_manager_);
  }

  void TearDown(const benchmark::State &state) final {
    txn_manager_->Commit(txn_, transaction::TransactionUtil::EmptyCallback, nullptr);

    EndGC();
    delete txn_manager_;
  }

  settings::SettingsManager *settings_manager_;

  storage::RecordBufferSegmentPool buffer_pool_{100, 100};

  transaction::TransactionContext *txn_ = nullptr;
  transaction::TransactionManager *txn_manager_;
  std::default_random_engine generator_;
  const uint8_t num_txns_ = 1;
  const uint32_t num_ops_ = 10000000;

  std::thread gc_thread_;
  storage::GarbageCollector *gc_ = nullptr;
  volatile bool run_gc_ = false;
  const std::chrono::milliseconds gc_period_{10};

  const std::string default_namespace_{"public"};
};

// After completing each transaction, an Aggregation is performed.
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(MetricBenchmark, AggregateMetric)(benchmark::State &state) {
  auto stats_collector = storage::metric::ThreadLevelStatsCollector();
  storage::metric::StatsAggregator aggregator(txn_manager_, nullptr);

  // NOLINTNEXTLINE
  for (auto _ : state) {
    for (uint8_t j = 0; j < num_txns_; j++) {
      auto *txn = txn_manager_->BeginTransaction();
      // Simulation of reading a tuple
      storage::metric::ThreadLevelStatsCollector::GetCollectorForThread()->CollectTupleRead(
          txn, catalog::db_oid_t(1), catalog::namespace_oid_t(2), catalog::table_oid_t(3));
      txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

      // The Aggregate() function accounts for more than 99.5% of the total running time for this benchmark.
      // So the throughput of this benchmark can be viewed as the throughput of the Aggeragate() function.
      aggregator.Aggregate(txn_);
    }
  }
  // The number of transactions is equal to the number of aggregations in this benchmark.
  state.SetItemsProcessed(state.iterations() * num_txns_);
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(MetricBenchmark, CollectMetric)(benchmark::State &state) {
  auto stats_collector = storage::metric::ThreadLevelStatsCollector();
  storage::metric::StatsAggregator aggregator(txn_manager_, nullptr);

  // NOLINTNEXTLINE
  for (auto _ : state) {
    for (uint8_t j = 0; j < num_txns_; j++) {
      auto *txn = txn_manager_->BeginTransaction();
      // Simulation of actions
      for (uint32_t k = 0; k < num_ops_; k++) {
        auto op_type = std::uniform_int_distribution<uint8_t>(0, 3)(generator_);
        if (op_type == 0) {  // Read
          storage::metric::ThreadLevelStatsCollector::GetCollectorForThread()->CollectTupleRead(
              txn, catalog::db_oid_t(1), catalog::namespace_oid_t(2), catalog::table_oid_t(3));
        } else if (op_type == 1) {  // Update
          storage::metric::ThreadLevelStatsCollector::GetCollectorForThread()->CollectTupleUpdate(
              txn, catalog::db_oid_t(1), catalog::namespace_oid_t(2), catalog::table_oid_t(3));
        } else if (op_type == 2) {  // Insert
          storage::metric::ThreadLevelStatsCollector::GetCollectorForThread()->CollectTupleInsert(
              txn, catalog::db_oid_t(1), catalog::namespace_oid_t(2), catalog::table_oid_t(3));
        } else {  // Delete
          storage::metric::ThreadLevelStatsCollector::GetCollectorForThread()->CollectTupleDelete(
              txn, catalog::db_oid_t(1), catalog::namespace_oid_t(2), catalog::table_oid_t(3));
        }
      }
      txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
    }

    aggregator.Aggregate(txn_);
  }
  state.SetItemsProcessed(state.iterations() * num_ops_ * num_txns_);
}

BENCHMARK_REGISTER_F(MetricBenchmark, AggregateMetric)->Unit(benchmark::kMillisecond)->UseRealTime();
BENCHMARK_REGISTER_F(MetricBenchmark, CollectMetric)->Unit(benchmark::kMillisecond)->UseRealTime();
}  // namespace terrier
