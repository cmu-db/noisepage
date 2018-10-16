#include <vector>
#include "benchmark/benchmark.h"
#include "common/scoped_timer.h"
#include "storage/garbage_collector.h"
#include "storage/storage_defs.h"
#include "storage/write_ahead_log/log_manager.h"
#include "util/transaction_benchmark_util.h"

#define LOG_FILE_NAME "/mnt/ramdisk/benchmark.txt"

namespace terrier {

class LoggingBenchmark : public benchmark::Fixture {
 public:
  void StartLogging() {
    logging_ = true;
    log_thread_ = std::thread([this] { LogThreadLoop(); });
  }

  void EndLogging() {
    logging_ = false;
    log_thread_.join();
    log_manager_->Shutdown();
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

  void TearDown(const benchmark::State &state) final { unlink(LOG_FILE_NAME); }

  const std::vector<uint8_t> attr_sizes = {8, 8, 8, 8, 8, 8, 8, 8, 8, 8};
  const uint32_t initial_table_size = 1000000;
  const uint32_t num_txns = 100000;
  storage::BlockStore block_store_{1000, 1000};
  storage::RecordBufferSegmentPool buffer_pool_{1000000, 1000000};
  std::default_random_engine generator_;
  const uint32_t num_concurrent_txns_ = 4;
  storage::LogManager *log_manager_ = nullptr;

 private:
  std::thread log_thread_;
  volatile bool logging_ = false;
  const std::chrono::milliseconds log_period_milli_{10};

  void LogThreadLoop() {
    while (logging_) {
      std::this_thread::sleep_for(log_period_milli_);
      log_manager_->Process();
    }
  }
  std::thread gc_thread_;
  storage::GarbageCollector *gc_ = nullptr;
  volatile bool run_gc_ = false;
  const std::chrono::milliseconds gc_period_{10};

  void GCThreadLoop() {
    while (run_gc_) {
      std::this_thread::sleep_for(gc_period_);
      gc_->PerformGarbageCollection();
    }
  }
};

/**
 * Run a TPCC-like workload (5 statements per txn, 10% insert, 40% update, 50% select).
 */
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(LoggingBenchmark, TPCCish)(benchmark::State &state) {
  uint64_t abort_count = 0;
  const uint32_t txn_length = 5;
  const std::vector<double> update_select_ratio = {0.1, 0.4, 0.5};
  // NOLINTNEXTLINE
  for (auto _ : state) {
    log_manager_ = new storage::LogManager(LOG_FILE_NAME, &buffer_pool_);
    LargeTransactionBenchmarkObject tested(attr_sizes, initial_table_size, txn_length, update_select_ratio,
                                           &block_store_, &buffer_pool_, &generator_, true, log_manager_);
    log_manager_->Process();  // log all of the Inserts from table creation
    StartGC(tested.GetTxnManager());
    StartLogging();
    uint64_t elapsed_ms;
    {
      common::ScopedTimer timer(&elapsed_ms);
      abort_count += tested.SimulateOltp(num_txns, num_concurrent_txns_);
    }
    state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);
    EndLogging();
    EndGC();
    delete log_manager_;
  }
  state.SetItemsProcessed(state.iterations() * num_txns - abort_count);
}

/**
 * Run a high number of statements with lots of updates to try to trigger aborts.
 */
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(LoggingBenchmark, HighAbortRate)(benchmark::State &state) {
  uint64_t abort_count = 0;
  const uint32_t txn_length = 40;
  const std::vector<double> update_select_ratio = {0.0, 0.8, 0.2};
  // NOLINTNEXTLINE
  for (auto _ : state) {
    // use a smaller table to make aborts more likely
    log_manager_ = new storage::LogManager(LOG_FILE_NAME, &buffer_pool_);
    LargeTransactionBenchmarkObject tested(attr_sizes, 1000, txn_length, update_select_ratio, &block_store_,
                                           &buffer_pool_, &generator_, true, log_manager_);
    log_manager_->Process();  // log all of the Inserts from table creation
    StartGC(tested.GetTxnManager());
    StartLogging();
    uint64_t elapsed_ms;
    {
      common::ScopedTimer timer(&elapsed_ms);
      abort_count += tested.SimulateOltp(num_txns, num_concurrent_txns_);
    }
    state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);
    EndLogging();
    EndGC();
    delete log_manager_;
  }
  state.SetItemsProcessed(state.iterations() * num_txns - abort_count);
}

/**
 * Single statement insert throughput. Should have no aborts.
 */
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(LoggingBenchmark, SingleStatementInsert)(benchmark::State &state) {
  uint64_t abort_count = 0;
  const uint32_t txn_length = 1;
  const std::vector<double> update_select_ratio = {1, 0, 0};
  // NOLINTNEXTLINE
  for (auto _ : state) {
    log_manager_ = new storage::LogManager(LOG_FILE_NAME, &buffer_pool_);
    LargeTransactionBenchmarkObject tested(attr_sizes, initial_table_size, txn_length, update_select_ratio,
                                           &block_store_, &buffer_pool_, &generator_, true, log_manager_);
    log_manager_->Process();  // log all of the Inserts from table creation
    StartGC(tested.GetTxnManager());
    StartLogging();
    uint64_t elapsed_ms;
    {
      common::ScopedTimer timer(&elapsed_ms);
      abort_count += tested.SimulateOltp(num_txns, num_concurrent_txns_);
    }
    state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);
    EndLogging();
    EndGC();
    delete log_manager_;
  }
  state.SetItemsProcessed(state.iterations() * num_txns - abort_count);
}

/**
 * Single statement update throughput. Should have low abort rates.
 */
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(LoggingBenchmark, SingleStatementUpdate)(benchmark::State &state) {
  uint64_t abort_count = 0;
  const uint32_t txn_length = 1;
  const std::vector<double> update_select_ratio = {0, 1, 0};
  // NOLINTNEXTLINE
  for (auto _ : state) {
    log_manager_ = new storage::LogManager(LOG_FILE_NAME, &buffer_pool_);
    LargeTransactionBenchmarkObject tested(attr_sizes, initial_table_size, txn_length, update_select_ratio,
                                           &block_store_, &buffer_pool_, &generator_, true, log_manager_);
    log_manager_->Process();  // log all of the Inserts from table creation
    StartGC(tested.GetTxnManager());
    StartLogging();
    uint64_t elapsed_ms;
    {
      common::ScopedTimer timer(&elapsed_ms);
      abort_count += tested.SimulateOltp(num_txns, num_concurrent_txns_);
    }
    state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);
    EndLogging();
    EndGC();
    delete log_manager_;
  }
  state.SetItemsProcessed(state.iterations() * num_txns - abort_count);
}

/**
 * Single statement update throughput. Should have no aborts.
 */
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(LoggingBenchmark, SingleStatementSelect)(benchmark::State &state) {
  uint64_t abort_count = 0;
  const uint32_t txn_length = 1;
  const std::vector<double> update_select_ratio = {0, 0, 1};
  // NOLINTNEXTLINE
  for (auto _ : state) {
    log_manager_ = new storage::LogManager(LOG_FILE_NAME, &buffer_pool_);
    LargeTransactionBenchmarkObject tested(attr_sizes, initial_table_size, txn_length, update_select_ratio,
                                           &block_store_, &buffer_pool_, &generator_, true, log_manager_);
    log_manager_->Process();  // log all of the Inserts from table creation
    StartGC(tested.GetTxnManager());
    StartLogging();
    uint64_t elapsed_ms;
    {
      common::ScopedTimer timer(&elapsed_ms);
      abort_count += tested.SimulateOltp(num_txns, num_concurrent_txns_);
    }
    state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);
    EndLogging();
    EndGC();
    delete log_manager_;
  }
  state.SetItemsProcessed(state.iterations() * num_txns - abort_count);
}

BENCHMARK_REGISTER_F(LoggingBenchmark, TPCCish)->Unit(benchmark::kMillisecond)->UseManualTime()->MinTime(3);

BENCHMARK_REGISTER_F(LoggingBenchmark, HighAbortRate)->Unit(benchmark::kMillisecond)->UseManualTime()->MinTime(10);

BENCHMARK_REGISTER_F(LoggingBenchmark, SingleStatementInsert)
    ->Unit(benchmark::kMillisecond)
    ->UseManualTime()
    ->MinTime(2);

BENCHMARK_REGISTER_F(LoggingBenchmark, SingleStatementUpdate)
    ->Unit(benchmark::kMillisecond)
    ->UseManualTime()
    ->MinTime(1);

BENCHMARK_REGISTER_F(LoggingBenchmark, SingleStatementSelect)
    ->Unit(benchmark::kMillisecond)
    ->UseManualTime()
    ->MinTime(1);
}  // namespace terrier
