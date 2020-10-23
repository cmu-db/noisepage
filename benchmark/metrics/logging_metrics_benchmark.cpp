#include <string>
#include <vector>

#include "benchmark/benchmark.h"
#include "benchmark_util/benchmark_config.h"
#include "benchmark_util/data_table_benchmark_util.h"
#include "common/dedicated_thread_registry.h"
#include "common/scoped_timer.h"
#include "metrics/metrics_thread.h"
#include "storage/garbage_collector_thread.h"
#include "storage/storage_defs.h"
#include "storage/write_ahead_log/log_manager.h"

namespace noisepage {

class LoggingMetricsBenchmark : public benchmark::Fixture {
 public:
  void TearDown(const benchmark::State &state) final { unlink(noisepage::BenchmarkConfig::logfile_path.data()); }

  const std::vector<uint16_t> attr_sizes_ = {8, 8, 8, 8, 8, 8, 8, 8, 8, 8};
  const uint32_t initial_table_size_ = 1000000;
  const uint32_t num_txns_ = 100000;
  storage::BlockStore block_store_{1000, 1000};
  storage::RecordBufferSegmentPool buffer_pool_{1000000, 1000000};
  std::default_random_engine generator_;
  storage::LogManager *log_manager_ = nullptr;
  storage::GarbageCollector *gc_ = nullptr;
  storage::GarbageCollectorThread *gc_thread_ = nullptr;
  const std::chrono::microseconds gc_period_{1000};
  const std::chrono::microseconds metrics_period_{10000};
  common::DedicatedThreadRegistry *thread_registry_ = nullptr;

  // Settings for log manager
  const uint64_t num_log_buffers_ = 100;
  const std::chrono::microseconds log_serialization_interval_{100};
  const std::chrono::microseconds log_persist_interval_{100};
  const uint64_t log_persist_threshold_ = (1 << 20);  // 1MB
};

/**
 * Run a TPCC-like workload (5 statements per txn, 10% insert, 40% update, 50% select).
 */
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(LoggingMetricsBenchmark, TPCCish)(benchmark::State &state) {
  uint64_t abort_count = 0;
  const uint32_t txn_length = 5;
  const std::vector<double> insert_update_select_ratio = {0.1, 0.4, 0.5};
  // NOLINTNEXTLINE
  for (auto _ : state) {
    unlink(noisepage::BenchmarkConfig::logfile_path.data());
    for (const auto &file : metrics::LoggingMetricRawData::FILES) unlink(std::string(file).c_str());
    auto *const metrics_manager = new metrics::MetricsManager();
    auto *const metrics_thread = new metrics::MetricsThread(common::ManagedPointer(metrics_manager), metrics_period_);
    metrics_manager->SetMetricSampleInterval(metrics::MetricsComponent::LOGGING, 0);
    metrics_manager->EnableMetric(metrics::MetricsComponent::LOGGING);
    thread_registry_ = new common::DedicatedThreadRegistry(common::ManagedPointer(metrics_manager));

    log_manager_ = new storage::LogManager(noisepage::BenchmarkConfig::logfile_path.data(), num_log_buffers_,
                                           log_serialization_interval_, log_persist_interval_, log_persist_threshold_,
                                           common::ManagedPointer(&buffer_pool_),
                                           common::ManagedPointer<common::DedicatedThreadRegistry>(thread_registry_));
    log_manager_->Start();
    LargeDataTableBenchmarkObject tested(attr_sizes_, initial_table_size_, txn_length, insert_update_select_ratio,
                                         &block_store_, &buffer_pool_, &generator_, true, log_manager_);
    // log all of the Inserts from table creation
    log_manager_->ForceFlush();

    gc_ = new storage::GarbageCollector(common::ManagedPointer(tested.GetTimestampManager()), DISABLED,
                                        common::ManagedPointer(tested.GetTxnManager()), DISABLED);
    gc_thread_ = new storage::GarbageCollectorThread(common::ManagedPointer(gc_), gc_period_, nullptr);
    const auto result = tested.SimulateOltp(num_txns_, noisepage::BenchmarkConfig::num_threads);
    abort_count += result.first;
    uint64_t elapsed_ms;
    {
      common::ScopedTimer<std::chrono::milliseconds> timer(&elapsed_ms);
      log_manager_->ForceFlush();
    }
    state.SetIterationTime(static_cast<double>(result.second + elapsed_ms) / 1000.0);
    log_manager_->PersistAndStop();
    delete log_manager_;
    delete gc_thread_;
    delete gc_;
    delete thread_registry_;
    delete metrics_thread;
    delete metrics_manager;
    unlink(noisepage::BenchmarkConfig::logfile_path.data());
  }
  state.SetItemsProcessed(state.iterations() * num_txns_ - abort_count);
}

/**
 * Run a high number of statements with lots of updates to try to trigger aborts.
 */
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(LoggingMetricsBenchmark, HighAbortRate)(benchmark::State &state) {
  uint64_t abort_count = 0;
  const uint32_t txn_length = 40;
  const std::vector<double> insert_update_select_ratio = {0.0, 0.8, 0.2};
  // NOLINTNEXTLINE
  for (auto _ : state) {
    unlink(noisepage::BenchmarkConfig::logfile_path.data());
    for (const auto &file : metrics::LoggingMetricRawData::FILES) unlink(std::string(file).c_str());
    auto *const metrics_manager = new metrics::MetricsManager();
    auto *const metrics_thread = new metrics::MetricsThread(common::ManagedPointer(metrics_manager), metrics_period_);
    metrics_manager->SetMetricSampleInterval(metrics::MetricsComponent::LOGGING, 0);
    metrics_manager->EnableMetric(metrics::MetricsComponent::LOGGING);
    thread_registry_ = new common::DedicatedThreadRegistry(common::ManagedPointer(metrics_manager));

    log_manager_ = new storage::LogManager(noisepage::BenchmarkConfig::logfile_path.data(), num_log_buffers_,
                                           log_serialization_interval_, log_persist_interval_, log_persist_threshold_,
                                           common::ManagedPointer(&buffer_pool_),
                                           common::ManagedPointer<common::DedicatedThreadRegistry>(thread_registry_));
    log_manager_->Start();
    LargeDataTableBenchmarkObject tested(attr_sizes_, 1000, txn_length, insert_update_select_ratio, &block_store_,
                                         &buffer_pool_, &generator_, true, log_manager_);
    // log all of the Inserts from table creation
    log_manager_->ForceFlush();

    gc_ = new storage::GarbageCollector(common::ManagedPointer(tested.GetTimestampManager()), DISABLED,
                                        common::ManagedPointer(tested.GetTxnManager()), DISABLED);
    gc_thread_ = new storage::GarbageCollectorThread(common::ManagedPointer(gc_), gc_period_, nullptr);
    const auto result = tested.SimulateOltp(num_txns_, noisepage::BenchmarkConfig::num_threads);
    abort_count += result.first;
    uint64_t elapsed_ms;
    {
      common::ScopedTimer<std::chrono::milliseconds> timer(&elapsed_ms);
      log_manager_->ForceFlush();
    }
    state.SetIterationTime(static_cast<double>(result.second + elapsed_ms) / 1000.0);
    log_manager_->PersistAndStop();
    delete log_manager_;
    delete gc_thread_;
    delete gc_;
    delete thread_registry_;
    delete metrics_thread;
    delete metrics_manager;
    unlink(noisepage::BenchmarkConfig::logfile_path.data());
  }
  state.SetItemsProcessed(state.iterations() * num_txns_ - abort_count);
}

/**
 * Single statement insert throughput. Should have no aborts.
 */
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(LoggingMetricsBenchmark, SingleStatementInsert)(benchmark::State &state) {
  uint64_t abort_count = 0;
  const uint32_t txn_length = 1;
  const std::vector<double> insert_update_select_ratio = {1, 0, 0};
  // NOLINTNEXTLINE
  for (auto _ : state) {
    unlink(noisepage::BenchmarkConfig::logfile_path.data());
    for (const auto &file : metrics::LoggingMetricRawData::FILES) unlink(std::string(file).c_str());
    auto *const metrics_manager = new metrics::MetricsManager();
    auto *const metrics_thread = new metrics::MetricsThread(common::ManagedPointer(metrics_manager), metrics_period_);
    metrics_manager->SetMetricSampleInterval(metrics::MetricsComponent::LOGGING, 0);
    metrics_manager->EnableMetric(metrics::MetricsComponent::LOGGING);
    thread_registry_ = new common::DedicatedThreadRegistry(common::ManagedPointer(metrics_manager));

    log_manager_ = new storage::LogManager(noisepage::BenchmarkConfig::logfile_path.data(), num_log_buffers_,
                                           log_serialization_interval_, log_persist_interval_, log_persist_threshold_,
                                           common::ManagedPointer(&buffer_pool_),
                                           common::ManagedPointer<common::DedicatedThreadRegistry>(thread_registry_));
    log_manager_->Start();
    LargeDataTableBenchmarkObject tested(attr_sizes_, 0, txn_length, insert_update_select_ratio, &block_store_,
                                         &buffer_pool_, &generator_, true, log_manager_);
    // log all of the Inserts from table creation
    log_manager_->ForceFlush();

    gc_ = new storage::GarbageCollector(common::ManagedPointer(tested.GetTimestampManager()), DISABLED,
                                        common::ManagedPointer(tested.GetTxnManager()), DISABLED);
    gc_thread_ = new storage::GarbageCollectorThread(common::ManagedPointer(gc_), gc_period_, nullptr);
    const auto result = tested.SimulateOltp(num_txns_, noisepage::BenchmarkConfig::num_threads);
    abort_count += result.first;
    uint64_t elapsed_ms;
    {
      common::ScopedTimer<std::chrono::milliseconds> timer(&elapsed_ms);
      log_manager_->ForceFlush();
    }
    state.SetIterationTime(static_cast<double>(result.second + elapsed_ms) / 1000.0);
    log_manager_->PersistAndStop();
    delete log_manager_;
    delete gc_thread_;
    delete gc_;
    delete thread_registry_;
    delete metrics_thread;
    delete metrics_manager;
    unlink(noisepage::BenchmarkConfig::logfile_path.data());
  }
  state.SetItemsProcessed(state.iterations() * num_txns_ - abort_count);
}

/**
 * Single statement update throughput. Should have low abort rates.
 */
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(LoggingMetricsBenchmark, SingleStatementUpdate)(benchmark::State &state) {
  uint64_t abort_count = 0;
  const uint32_t txn_length = 1;
  const std::vector<double> insert_update_select_ratio = {0, 1, 0};
  // NOLINTNEXTLINE
  for (auto _ : state) {
    unlink(noisepage::BenchmarkConfig::logfile_path.data());
    for (const auto &file : metrics::LoggingMetricRawData::FILES) unlink(std::string(file).c_str());
    auto *const metrics_manager = new metrics::MetricsManager();
    auto *const metrics_thread = new metrics::MetricsThread(common::ManagedPointer(metrics_manager), metrics_period_);
    metrics_manager->SetMetricSampleInterval(metrics::MetricsComponent::LOGGING, 0);
    metrics_manager->EnableMetric(metrics::MetricsComponent::LOGGING);
    thread_registry_ = new common::DedicatedThreadRegistry(common::ManagedPointer(metrics_manager));

    log_manager_ = new storage::LogManager(noisepage::BenchmarkConfig::logfile_path.data(), num_log_buffers_,
                                           log_serialization_interval_, log_persist_interval_, log_persist_threshold_,
                                           common::ManagedPointer(&buffer_pool_),
                                           common::ManagedPointer<common::DedicatedThreadRegistry>(thread_registry_));
    log_manager_->Start();
    LargeDataTableBenchmarkObject tested(attr_sizes_, initial_table_size_, txn_length, insert_update_select_ratio,
                                         &block_store_, &buffer_pool_, &generator_, true, log_manager_);
    // log all of the Inserts from table creation
    log_manager_->ForceFlush();

    gc_ = new storage::GarbageCollector(common::ManagedPointer(tested.GetTimestampManager()), DISABLED,
                                        common::ManagedPointer(tested.GetTxnManager()), DISABLED);
    gc_thread_ = new storage::GarbageCollectorThread(common::ManagedPointer(gc_), gc_period_, nullptr);
    const auto result = tested.SimulateOltp(num_txns_, noisepage::BenchmarkConfig::num_threads);
    abort_count += result.first;
    uint64_t elapsed_ms;
    {
      common::ScopedTimer<std::chrono::milliseconds> timer(&elapsed_ms);
      log_manager_->ForceFlush();
    }
    state.SetIterationTime(static_cast<double>(result.second + elapsed_ms) / 1000.0);
    log_manager_->PersistAndStop();
    delete log_manager_;
    delete gc_thread_;
    delete gc_;
    delete thread_registry_;
    delete metrics_thread;
    delete metrics_manager;
    unlink(noisepage::BenchmarkConfig::logfile_path.data());
  }
  state.SetItemsProcessed(state.iterations() * num_txns_ - abort_count);
}

/**
 * Single statement select throughput. Should have no aborts.
 */
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(LoggingMetricsBenchmark, SingleStatementSelect)(benchmark::State &state) {
  uint64_t abort_count = 0;
  const uint32_t txn_length = 1;
  const std::vector<double> insert_update_select_ratio = {0, 0, 1};
  // NOLINTNEXTLINE
  for (auto _ : state) {
    unlink(noisepage::BenchmarkConfig::logfile_path.data());
    for (const auto &file : metrics::LoggingMetricRawData::FILES) unlink(std::string(file).c_str());
    auto *const metrics_manager = new metrics::MetricsManager();
    auto *const metrics_thread = new metrics::MetricsThread(common::ManagedPointer(metrics_manager), metrics_period_);
    metrics_manager->SetMetricSampleInterval(metrics::MetricsComponent::LOGGING, 0);
    metrics_manager->EnableMetric(metrics::MetricsComponent::LOGGING);
    thread_registry_ = new common::DedicatedThreadRegistry(common::ManagedPointer(metrics_manager));

    log_manager_ = new storage::LogManager(noisepage::BenchmarkConfig::logfile_path.data(), num_log_buffers_,
                                           log_serialization_interval_, log_persist_interval_, log_persist_threshold_,
                                           common::ManagedPointer(&buffer_pool_),
                                           common::ManagedPointer<common::DedicatedThreadRegistry>(thread_registry_));
    log_manager_->Start();
    LargeDataTableBenchmarkObject tested(attr_sizes_, initial_table_size_, txn_length, insert_update_select_ratio,
                                         &block_store_, &buffer_pool_, &generator_, true, log_manager_);
    // log all of the Inserts from table creation
    log_manager_->ForceFlush();

    gc_ = new storage::GarbageCollector(common::ManagedPointer(tested.GetTimestampManager()), DISABLED,
                                        common::ManagedPointer(tested.GetTxnManager()), DISABLED);
    gc_thread_ = new storage::GarbageCollectorThread(common::ManagedPointer(gc_), gc_period_, nullptr);
    const auto result = tested.SimulateOltp(num_txns_, noisepage::BenchmarkConfig::num_threads);
    abort_count += result.first;
    uint64_t elapsed_ms;
    {
      common::ScopedTimer<std::chrono::milliseconds> timer(&elapsed_ms);
      log_manager_->ForceFlush();
    }
    state.SetIterationTime(static_cast<double>(result.second + elapsed_ms) / 1000.0);
    log_manager_->PersistAndStop();
    delete log_manager_;
    delete gc_thread_;
    delete gc_;
    delete thread_registry_;
    delete metrics_thread;
    delete metrics_manager;
    unlink(noisepage::BenchmarkConfig::logfile_path.data());
  }
  state.SetItemsProcessed(state.iterations() * num_txns_ - abort_count);
}

BENCHMARK_REGISTER_F(LoggingMetricsBenchmark, TPCCish)->Unit(benchmark::kMillisecond)->UseManualTime()->MinTime(3);

BENCHMARK_REGISTER_F(LoggingMetricsBenchmark, HighAbortRate)
    ->Unit(benchmark::kMillisecond)
    ->UseManualTime()
    ->MinTime(10);

BENCHMARK_REGISTER_F(LoggingMetricsBenchmark, SingleStatementInsert)
    ->Unit(benchmark::kMillisecond)
    ->UseManualTime()
    ->MinTime(2);

BENCHMARK_REGISTER_F(LoggingMetricsBenchmark, SingleStatementUpdate)
    ->Unit(benchmark::kMillisecond)
    ->UseManualTime()
    ->MinTime(1);

BENCHMARK_REGISTER_F(LoggingMetricsBenchmark, SingleStatementSelect)
    ->Unit(benchmark::kMillisecond)
    ->UseManualTime()
    ->MinTime(1);
}  // namespace noisepage
