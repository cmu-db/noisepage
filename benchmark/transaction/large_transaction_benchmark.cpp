#include <vector>
#include "benchmark/benchmark.h"
#include "common/scoped_timer.h"
#include "storage/garbage_collector.h"
#include "util/transaction_benchmark_util.h"

namespace terrier {

class LargeTransactionBenchmark : public benchmark::Fixture {
 public:
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

  const std::vector<uint8_t> attr_sizes = {8, 8, 8, 8, 8, 8, 8, 8, 8, 8};
  const uint32_t initial_table_size = 1000000;
  const uint32_t num_txns = 100000;
  storage::BlockStore block_store_{1000, 1000};
  storage::RecordBufferSegmentPool buffer_pool_{1000000, 1000000};
  std::default_random_engine generator_;
  const uint32_t num_concurrent_txns_ = 4;

 private:
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
BENCHMARK_DEFINE_F(LargeTransactionBenchmark, TPCCish)(benchmark::State &state) {
  uint64_t abort_count = 0;
  const uint32_t txn_length = 5;
  const std::vector<double> insert_update_select_ratio = {0.1, 0.4, 0.5};
  // NOLINTNEXTLINE
  for (auto _ : state) {
    LargeTransactionBenchmarkObject tested(attr_sizes, initial_table_size, txn_length, insert_update_select_ratio,
                                           &block_store_, &buffer_pool_, &generator_, true);
    StartGC(tested.GetTxnManager());
    uint64_t elapsed_ms;
    {
      common::ScopedTimer timer(&elapsed_ms);
      abort_count += tested.SimulateOltp(num_txns, num_concurrent_txns_);
    }
    state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);
    EndGC();
  }
  state.SetItemsProcessed(state.iterations() * num_txns - abort_count);
}

/**
 * Run a high number of statements with lots of updates to try to trigger aborts.
 */
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(LargeTransactionBenchmark, HighAbortRate)(benchmark::State &state) {
  uint64_t abort_count = 0;
  const uint32_t txn_length = 40;
  const std::vector<double> insert_update_select_ratio = {0.0, 0.8, 0.2};
  // NOLINTNEXTLINE
  for (auto _ : state) {
    // use a smaller table to make aborts more likely
    LargeTransactionBenchmarkObject tested(attr_sizes, 1000, txn_length, insert_update_select_ratio, &block_store_,
                                           &buffer_pool_, &generator_, true);
    StartGC(tested.GetTxnManager());
    uint64_t elapsed_ms;
    {
      common::ScopedTimer timer(&elapsed_ms);
      abort_count += tested.SimulateOltp(num_txns, num_concurrent_txns_);
    }
    state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);
    EndGC();
  }
  state.SetItemsProcessed(state.iterations() * num_txns - abort_count);
}

/**
 * Single statement insert throughput. Should have no aborts.
 */
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(LargeTransactionBenchmark, SingleStatementInsert)(benchmark::State &state) {
  uint64_t abort_count = 0;
  const uint32_t txn_length = 1;
  const std::vector<double> insert_update_select_ratio = {1, 0, 0};
  // NOLINTNEXTLINE
  for (auto _ : state) {
    // don't need any initial tuples
    LargeTransactionBenchmarkObject tested(attr_sizes, 0, txn_length, insert_update_select_ratio, &block_store_,
                                           &buffer_pool_, &generator_, true);
    StartGC(tested.GetTxnManager());
    uint64_t elapsed_ms;
    {
      common::ScopedTimer timer(&elapsed_ms);
      abort_count += tested.SimulateOltp(num_txns, num_concurrent_txns_);
    }
    state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);
    EndGC();
  }
  state.SetItemsProcessed(state.iterations() * num_txns - abort_count);
}

/**
 * Single statement update throughput. Should have low abort rates.
 */
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(LargeTransactionBenchmark, SingleStatementUpdate)(benchmark::State &state) {
  uint64_t abort_count = 0;
  const uint32_t txn_length = 1;
  const std::vector<double> insert_update_select_ratio = {0, 1, 0};
  // NOLINTNEXTLINE
  for (auto _ : state) {
    LargeTransactionBenchmarkObject tested(attr_sizes, initial_table_size, txn_length, insert_update_select_ratio,
                                           &block_store_, &buffer_pool_, &generator_, true);
    StartGC(tested.GetTxnManager());
    uint64_t elapsed_ms;
    {
      common::ScopedTimer timer(&elapsed_ms);
      abort_count += tested.SimulateOltp(num_txns, num_concurrent_txns_);
    }
    state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);
    EndGC();
  }
  state.SetItemsProcessed(state.iterations() * num_txns - abort_count);
}

/**
 * Single statement select throughput. Should have no aborts.
 */
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(LargeTransactionBenchmark, SingleStatementSelect)(benchmark::State &state) {
  uint64_t abort_count = 0;
  const uint32_t txn_length = 1;
  const std::vector<double> insert_update_select_ratio = {0, 0, 1};
  // NOLINTNEXTLINE
  for (auto _ : state) {
    LargeTransactionBenchmarkObject tested(attr_sizes, initial_table_size, txn_length, insert_update_select_ratio,
                                           &block_store_, &buffer_pool_, &generator_, true);
    StartGC(tested.GetTxnManager());
    uint64_t elapsed_ms;
    {
      common::ScopedTimer timer(&elapsed_ms);
      abort_count += tested.SimulateOltp(num_txns, num_concurrent_txns_);
    }
    state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);
    EndGC();
  }
  state.SetItemsProcessed(state.iterations() * num_txns - abort_count);
}

BENCHMARK_REGISTER_F(LargeTransactionBenchmark, TPCCish)->Unit(benchmark::kMillisecond)->UseManualTime()->MinTime(3);

 BENCHMARK_REGISTER_F(LargeTransactionBenchmark, HighAbortRate)
    ->Unit(benchmark::kMillisecond)
    ->UseManualTime()
    ->MinTime(10);

BENCHMARK_REGISTER_F(LargeTransactionBenchmark, SingleStatementInsert)
    ->Unit(benchmark::kMillisecond)
    ->UseManualTime()
    ->MinTime(2);

BENCHMARK_REGISTER_F(LargeTransactionBenchmark, SingleStatementUpdate)
    ->Unit(benchmark::kMillisecond)
    ->UseManualTime()
    ->MinTime(1);

BENCHMARK_REGISTER_F(LargeTransactionBenchmark, SingleStatementSelect)
    ->Unit(benchmark::kMillisecond)
    ->UseManualTime()
    ->MinTime(1);
}  // namespace terrier
