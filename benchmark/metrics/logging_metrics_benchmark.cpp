#include <string>
#include <vector>

#include "benchmark/benchmark.h"
#include "benchmark_util/data_table_benchmark_util.h"
#include "common/scoped_timer.h"
#include "main/db_main.h"
#include "metrics/metrics_manager.h"
#include "storage/storage_defs.h"

#define LOG_FILE_NAME "/mnt/ramdisk/benchmark.txt"

namespace terrier {

class LoggingMetricsBenchmark : public benchmark::Fixture {
 public:
  void SetUp(const benchmark::State &state) final { unlink(LOG_FILE_NAME); }
  void TearDown(const benchmark::State &state) final { unlink(LOG_FILE_NAME); }

  const std::vector<uint16_t> attr_sizes_ = {8, 8, 8, 8, 8, 8, 8, 8, 8, 8};
  const uint32_t initial_table_size_ = 1000000;
  const uint32_t num_txns_ = 100000;
  std::default_random_engine generator_;
  const uint32_t num_concurrent_txns_ = 4;
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
    unlink(LOG_FILE_NAME);
    for (const auto &file : metrics::LoggingMetricRawData::FILES) unlink(std::string(file).c_str());

    // Initialize table and run workload with logging enabled
    auto db_main = terrier::DBMain::Builder()
                       .SetLogFilePath(LOG_FILE_NAME)
                       .SetUseLogging(true)
                       .SetUseMetrics(true)
                       .SetUseMetricsThread(true)
                       .SetUseGC(true)
                       .SetUseGCThread(true)
                       .SetRecordBufferSegmentSize(1e6)
                       .SetRecordBufferSegmentReuse(1e6)
                       .Build();
    auto log_manager = db_main->GetLogManager();
    auto block_store = db_main->GetStorageLayer()->GetBlockStore();

    db_main->GetMetricsManager()->EnableMetric(metrics::MetricsComponent::LOGGING);

    auto *const tested = new LargeDataTableBenchmarkObject(
        attr_sizes_, initial_table_size_, txn_length, insert_update_select_ratio, block_store.Get(),
        db_main->GetBufferSegmentPool().Get(), &generator_, true, log_manager.Get());
    // log all of the Inserts from table creation
    log_manager->ForceFlush();

    const auto result = tested->SimulateOltp(num_txns_, num_concurrent_txns_);
    abort_count += result.first;
    uint64_t elapsed_ms;
    {
      common::ScopedTimer<std::chrono::milliseconds> timer(&elapsed_ms);
      log_manager->ForceFlush();
    }
    state.SetIterationTime(static_cast<double>(result.second + elapsed_ms) / 1000.0);
    db_main->GetTransactionLayer()->GetDeferredActionManager()->RegisterDeferredAction([=]() {
      delete tested;
      unlink(LOG_FILE_NAME);
    });
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
    unlink(LOG_FILE_NAME);
    for (const auto &file : metrics::LoggingMetricRawData::FILES) unlink(std::string(file).c_str());

    // Initialize table and run workload with logging enabled
    auto db_main = terrier::DBMain::Builder()
                       .SetLogFilePath(LOG_FILE_NAME)
                       .SetUseLogging(true)
                       .SetUseMetrics(true)
                       .SetUseMetricsThread(true)
                       .SetUseGC(true)
                       .SetUseGCThread(true)
                       .SetRecordBufferSegmentSize(1e6)
                       .SetRecordBufferSegmentReuse(1e6)
                       .Build();
    auto log_manager = db_main->GetLogManager();
    auto block_store = db_main->GetStorageLayer()->GetBlockStore();

    db_main->GetMetricsManager()->EnableMetric(metrics::MetricsComponent::LOGGING);

    auto *const tested =
        new LargeDataTableBenchmarkObject(attr_sizes_, 1000, txn_length, insert_update_select_ratio, block_store.Get(),
                                          db_main->GetBufferSegmentPool().Get(), &generator_, true, log_manager.Get());
    // log all of the Inserts from table creation
    log_manager->ForceFlush();

    const auto result = tested->SimulateOltp(num_txns_, num_concurrent_txns_);
    abort_count += result.first;
    uint64_t elapsed_ms;
    {
      common::ScopedTimer<std::chrono::milliseconds> timer(&elapsed_ms);
      log_manager->ForceFlush();
    }
    state.SetIterationTime(static_cast<double>(result.second + elapsed_ms) / 1000.0);
    db_main->GetTransactionLayer()->GetDeferredActionManager()->RegisterDeferredAction([=]() {
      delete tested;
      unlink(LOG_FILE_NAME);
    });
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
    unlink(LOG_FILE_NAME);
    for (const auto &file : metrics::LoggingMetricRawData::FILES) unlink(std::string(file).c_str());

    // Initialize table and run workload with logging enabled
    auto db_main = terrier::DBMain::Builder()
                       .SetLogFilePath(LOG_FILE_NAME)
                       .SetUseLogging(true)
                       .SetUseMetrics(true)
                       .SetUseMetricsThread(true)
                       .SetUseGC(true)
                       .SetUseGCThread(true)
                       .SetRecordBufferSegmentSize(1e6)
                       .SetRecordBufferSegmentReuse(1e6)
                       .Build();
    auto log_manager = db_main->GetLogManager();
    auto block_store = db_main->GetStorageLayer()->GetBlockStore();

    db_main->GetMetricsManager()->EnableMetric(metrics::MetricsComponent::LOGGING);

    auto *const tested = new LargeDataTableBenchmarkObject(
        attr_sizes_, initial_table_size_, txn_length, insert_update_select_ratio, block_store.Get(),
        db_main->GetBufferSegmentPool().Get(), &generator_, true, log_manager.Get());
    // log all of the Inserts from table creation
    log_manager->ForceFlush();

    const auto result = tested->SimulateOltp(num_txns_, num_concurrent_txns_);
    abort_count += result.first;
    uint64_t elapsed_ms;
    {
      common::ScopedTimer<std::chrono::milliseconds> timer(&elapsed_ms);
      log_manager->ForceFlush();
    }
    state.SetIterationTime(static_cast<double>(result.second + elapsed_ms) / 1000.0);
    db_main->GetTransactionLayer()->GetDeferredActionManager()->RegisterDeferredAction([=]() {
      delete tested;
      unlink(LOG_FILE_NAME);
    });
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
    unlink(LOG_FILE_NAME);
    for (const auto &file : metrics::LoggingMetricRawData::FILES) unlink(std::string(file).c_str());

    // Initialize table and run workload with logging enabled
    auto db_main = terrier::DBMain::Builder()
                       .SetLogFilePath(LOG_FILE_NAME)
                       .SetUseLogging(true)
                       .SetUseMetrics(true)
                       .SetUseMetricsThread(true)
                       .SetUseGC(true)
                       .SetUseGCThread(true)
                       .SetRecordBufferSegmentSize(1e6)
                       .SetRecordBufferSegmentReuse(1e6)
                       .Build();
    auto log_manager = db_main->GetLogManager();
    auto block_store = db_main->GetStorageLayer()->GetBlockStore();

    db_main->GetMetricsManager()->EnableMetric(metrics::MetricsComponent::LOGGING);

    auto *const tested = new LargeDataTableBenchmarkObject(
        attr_sizes_, initial_table_size_, txn_length, insert_update_select_ratio, block_store.Get(),
        db_main->GetBufferSegmentPool().Get(), &generator_, true, log_manager.Get());
    // log all of the Inserts from table creation
    log_manager->ForceFlush();

    const auto result = tested->SimulateOltp(num_txns_, num_concurrent_txns_);
    abort_count += result.first;
    uint64_t elapsed_ms;
    {
      common::ScopedTimer<std::chrono::milliseconds> timer(&elapsed_ms);
      log_manager->ForceFlush();
    }
    state.SetIterationTime(static_cast<double>(result.second + elapsed_ms) / 1000.0);
    db_main->GetTransactionLayer()->GetDeferredActionManager()->RegisterDeferredAction([=]() {
      delete tested;
      unlink(LOG_FILE_NAME);
    });
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
    unlink(LOG_FILE_NAME);
    for (const auto &file : metrics::LoggingMetricRawData::FILES) unlink(std::string(file).c_str());

    // Initialize table and run workload with logging enabled
    auto db_main = terrier::DBMain::Builder()
                       .SetLogFilePath(LOG_FILE_NAME)
                       .SetUseLogging(true)
                       .SetUseMetrics(true)
                       .SetUseMetricsThread(true)
                       .SetUseGC(true)
                       .SetUseGCThread(true)
                       .SetRecordBufferSegmentSize(1e6)
                       .SetRecordBufferSegmentReuse(1e6)
                       .Build();
    auto log_manager = db_main->GetLogManager();
    auto block_store = db_main->GetStorageLayer()->GetBlockStore();

    db_main->GetMetricsManager()->EnableMetric(metrics::MetricsComponent::LOGGING);

    auto *const tested = new LargeDataTableBenchmarkObject(
        attr_sizes_, initial_table_size_, txn_length, insert_update_select_ratio, block_store.Get(),
        db_main->GetBufferSegmentPool().Get(), &generator_, true, log_manager.Get());
    // log all of the Inserts from table creation
    log_manager->ForceFlush();

    const auto result = tested->SimulateOltp(num_txns_, num_concurrent_txns_);
    abort_count += result.first;
    uint64_t elapsed_ms;
    {
      common::ScopedTimer<std::chrono::milliseconds> timer(&elapsed_ms);
      log_manager->ForceFlush();
    }
    state.SetIterationTime(static_cast<double>(result.second + elapsed_ms) / 1000.0);
    db_main->GetTransactionLayer()->GetDeferredActionManager()->RegisterDeferredAction([=]() {
      delete tested;
      unlink(LOG_FILE_NAME);
    });
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
}  // namespace terrier
