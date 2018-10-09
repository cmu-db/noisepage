#include <vector>
#include "benchmark/benchmark.h"
#include "common/scoped_timer.h"
#include "util/transaction_benchmark_util.h"

namespace terrier {

class LargeTransactionBenchmark : public benchmark::Fixture {
 public:
  const std::vector<uint8_t> attr_sizes = {8, 8, 8, 8, 8, 8, 8, 8, 8, 8};
  const uint32_t initial_table_size = 1000000;
  const uint32_t num_txns = 500000;
  storage::BlockStore block_store_{1000, 1000};
  storage::RecordBufferSegmentPool buffer_pool_{1000000, 1000000};
  std::default_random_engine generator_;
  const uint32_t num_concurrent_txns_ = 4;
};

/**
 * Run a TPCC-like workload (5 statements per txn, 40% update, 60% select). We can't interleave inserts in this
 * framework yet so this it the best we can do.
 */
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(LargeTransactionBenchmark, TPCCish)(benchmark::State &state) {
  uint64_t abort_count = 0;
  const uint32_t txn_length = 5;
  const std::vector<double> update_select_ratio = {0.4, 0.6};
  // NOLINTNEXTLINE
  for (auto _ : state) {
    LargeTransactionBenchmarkObject tested(attr_sizes, initial_table_size, txn_length, update_select_ratio,
                                           &block_store_, &buffer_pool_, &generator_, false);
    uint64_t elapsed_ms;
    {
      common::ScopedTimer timer(&elapsed_ms);
      abort_count += tested.SimulateOltp(num_txns, num_concurrent_txns_);
    }
    state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);
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
  const std::vector<double> update_select_ratio = {0.8, 0.2};
  // NOLINTNEXTLINE
  for (auto _ : state) {
    // use a smaller table to make aborts more likely
    LargeTransactionBenchmarkObject tested(attr_sizes, 1000, txn_length, update_select_ratio, &block_store_,
                                           &buffer_pool_, &generator_, false);
    uint64_t elapsed_ms;
    {
      common::ScopedTimer timer(&elapsed_ms);
      abort_count += tested.SimulateOltp(num_txns, num_concurrent_txns_);
    }
    state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);
  }
  state.SetItemsProcessed(state.iterations() * num_txns - abort_count);
}

/**
 * Single statement insert throughput. Should have no aborts.
 */
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(LargeTransactionBenchmark, SingleStatementInsert)(benchmark::State &state) {
  TestThreadPool thread_pool;
  transaction::TransactionManager txn_manager(&buffer_pool_, false, LOGGING_DISABLED);
  const storage::BlockLayout layout{attr_sizes};
  const storage::ProjectedRowInitializer initializer_{layout, StorageTestUtil::ProjectionListAllColumns(layout)};
  // NOLINTNEXTLINE
  for (auto _ : state) {
    storage::DataTable table(&block_store_, layout, layout_version_t(0));

    auto workload = [&](uint32_t id) {
      // generate a random redo ProjectedRow to Insert
      byte *redo_buffer = common::AllocationUtil::AllocateAligned(initializer_.ProjectedRowSize());
      storage::ProjectedRow *redo = initializer_.InitializeRow(redo_buffer);
      StorageTestUtil::PopulateRandomRow(redo, layout, 0, &generator_);

      for (uint32_t i = 0; i < num_txns / num_concurrent_txns_; i++) {
        auto *txn = txn_manager.BeginTransaction();
        table.Insert(txn, *redo);
        txn_manager.Commit(txn, [] {});
        delete txn;
      }

      delete[] redo_buffer;
    };

    uint64_t elapsed_ms;
    {
      common::ScopedTimer timer(&elapsed_ms);
      thread_pool.RunThreadsUntilFinish(num_concurrent_txns_, workload);
    }

    state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);
  }
  state.SetItemsProcessed(state.iterations() * num_txns);
}

/**
 * Single statement update throughput. Should have low abort rates.
 */
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(LargeTransactionBenchmark, SingleStatementUpdate)(benchmark::State &state) {
  uint64_t abort_count = 0;
  const uint32_t txn_length = 1;
  const std::vector<double> update_select_ratio = {1.0, 0.0};
  // NOLINTNEXTLINE
  for (auto _ : state) {
    LargeTransactionBenchmarkObject tested(attr_sizes, initial_table_size, txn_length, update_select_ratio,
                                           &block_store_, &buffer_pool_, &generator_, false);
    uint64_t elapsed_ms;
    {
      common::ScopedTimer timer(&elapsed_ms);
      abort_count += tested.SimulateOltp(num_txns, num_concurrent_txns_);
    }
    state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);
  }
  state.SetItemsProcessed(state.iterations() * num_txns - abort_count);
}

/**
 * Single statement update throughput. Should have no aborts.
 */
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(LargeTransactionBenchmark, SingleStatementSelect)(benchmark::State &state) {
  uint64_t abort_count = 0;
  const uint32_t txn_length = 1;
  const std::vector<double> update_select_ratio = {0.0, 1.0};
  // NOLINTNEXTLINE
  for (auto _ : state) {
    LargeTransactionBenchmarkObject tested(attr_sizes, initial_table_size, txn_length, update_select_ratio,
                                           &block_store_, &buffer_pool_, &generator_, false);
    uint64_t elapsed_ms;
    {
      common::ScopedTimer timer(&elapsed_ms);
      abort_count += tested.SimulateOltp(num_txns, num_concurrent_txns_);
    }
    state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);
  }
  state.SetItemsProcessed(state.iterations() * num_txns - abort_count);
}

BENCHMARK_REGISTER_F(LargeTransactionBenchmark, TPCCish)->Unit(benchmark::kMillisecond)->UseManualTime()->MinTime(5);

BENCHMARK_REGISTER_F(LargeTransactionBenchmark, HighAbortRate)
    ->Unit(benchmark::kMillisecond)
    ->UseManualTime()
    ->MinTime(10);

BENCHMARK_REGISTER_F(LargeTransactionBenchmark, SingleStatementInsert)
    ->Unit(benchmark::kMillisecond)
    ->UseManualTime()
    ->MinTime(5);

BENCHMARK_REGISTER_F(LargeTransactionBenchmark, SingleStatementUpdate)
    ->Unit(benchmark::kMillisecond)
    ->UseManualTime()
    ->MinTime(5);

BENCHMARK_REGISTER_F(LargeTransactionBenchmark, SingleStatementSelect)
    ->Unit(benchmark::kMillisecond)
    ->UseManualTime()
    ->MinTime(5);
}  // namespace terrier
