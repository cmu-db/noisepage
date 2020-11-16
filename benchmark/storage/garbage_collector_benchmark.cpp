#include <utility>
#include <vector>

#include "benchmark/benchmark.h"
#include "benchmark_util/data_table_benchmark_util.h"
#include "common/scoped_timer.h"
#include "storage/garbage_collector.h"

namespace noisepage {

class GarbageCollectorBenchmark : public benchmark::Fixture {
 public:
  void StartGC(transaction::TimestampManager *const timestamp_manager,
               transaction::TransactionManager *const txn_manager) {
    gc_ = new storage::GarbageCollector(common::ManagedPointer(timestamp_manager), DISABLED,
                                        common::ManagedPointer(txn_manager), DISABLED);
    run_gc_ = true;
    gc_thread_ = std::thread([this] { GCThreadLoop(); });
  }

  uint32_t EndGC() {
    run_gc_ = false;
    gc_thread_.join();
    // Make sure all garbage is collected. This take 2 runs for unlink and deallocate
    gc_->PerformGarbageCollection();
    const uint32_t lag_count = gc_->PerformGarbageCollection().first;
    delete gc_;
    return lag_count;
  }

  const uint32_t txn_length_ = 5;
  const std::vector<double> update_select_ratio_ = {0, 1, 0};
  const uint32_t num_concurrent_txns_ = 4;
  const uint32_t initial_table_size_ = 100000;
  const uint32_t num_txns_ = 100000;
  storage::BlockStore block_store_{1000, 1000};
  storage::RecordBufferSegmentPool buffer_pool_{1000000, 1000000};
  std::default_random_engine generator_;
  storage::GarbageCollector *gc_ = nullptr;

 private:
  std::thread gc_thread_;
  volatile bool run_gc_ = false;
  const std::chrono::milliseconds gc_period_{10};

  void GCThreadLoop() {
    while (run_gc_) {
      std::this_thread::sleep_for(gc_period_);
      gc_->PerformGarbageCollection();
    }
  }
};

// Create a table with 100,000 tuples, then run 100,000 txns running update statements. Then run GC and profile how long
// the unlinking stage takes for those txns
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(GarbageCollectorBenchmark, UnlinkTime)(benchmark::State &state) {
  // NOLINTNEXTLINE
  for (auto _ : state) {
    // generate our table and instantiate GC
    LargeDataTableBenchmarkObject tested({8, 8, 8}, initial_table_size_, txn_length_, update_select_ratio_,
                                         &block_store_, &buffer_pool_, &generator_, true);
    gc_ = new storage::GarbageCollector(common::ManagedPointer(tested.GetTimestampManager()), DISABLED,
                                        common::ManagedPointer(tested.GetTxnManager()), DISABLED);

    // clean up insert txn
    gc_->PerformGarbageCollection();
    gc_->PerformGarbageCollection();

    // run all txns
    tested.SimulateOltp(num_txns_, num_concurrent_txns_);

    // time just the unlinking process, verify nothing deallocated
    uint64_t elapsed_ms;
    std::pair<uint32_t, uint32_t> result;
    {
      common::ScopedTimer<std::chrono::milliseconds> timer(&elapsed_ms);
      result = gc_->PerformGarbageCollection();
    }
    EXPECT_EQ(result.first, 0);
    EXPECT_EQ(result.second, num_txns_);

    // run another GC pass to perform deallocation, verify nothing unlinked
    result = gc_->PerformGarbageCollection();
    EXPECT_EQ(result.second, 0);

    delete gc_;

    state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);
  }
  state.SetItemsProcessed(state.iterations() * num_txns_);
}

// Create a table with 100,000 tuples, then run 100,000 txns running update statements. Then run GC and profile how long
// the deallocation stage takes for those txns
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(GarbageCollectorBenchmark, ReclaimTime)(benchmark::State &state) {
  // NOLINTNEXTLINE
  for (auto _ : state) {
    // generate our table and instantiate GC
    LargeDataTableBenchmarkObject tested({8, 8, 8}, initial_table_size_, txn_length_, update_select_ratio_,
                                         &block_store_, &buffer_pool_, &generator_, true);
    gc_ = new storage::GarbageCollector(common::ManagedPointer(tested.GetTimestampManager()), DISABLED,
                                        common::ManagedPointer(tested.GetTxnManager()), DISABLED);

    // clean up insert txn
    gc_->PerformGarbageCollection();
    gc_->PerformGarbageCollection();

    // run all txns
    tested.SimulateOltp(num_txns_, num_concurrent_txns_);

    // run first pass to unlink everything, verify nothing deallocated
    std::pair<uint32_t, uint32_t> result = gc_->PerformGarbageCollection();
    EXPECT_EQ(result.first, 0);

    // time just the deallocation process, verify nothing unlinked
    uint64_t elapsed_ms;
    {
      common::ScopedTimer<std::chrono::milliseconds> timer(&elapsed_ms);
      result = gc_->PerformGarbageCollection();
    }
    EXPECT_EQ(result.first, num_txns_);
    EXPECT_EQ(result.second, 0);

    delete gc_;

    state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);
  }
  state.SetItemsProcessed(state.iterations() * num_txns_);
}

/**
 * Run a large number of updates on a small table to generate contention with the GC. Measure the number of transactions
 * that the GC managed to free during the workload by subtracting the number of "lagging" transactions that still
 * remained to be cleaned up by the GC after the workload was done running.
 */
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(GarbageCollectorBenchmark, HighContention)(benchmark::State &state) {
  uint64_t lag_count = 0;
  // NOLINTNEXTLINE
  for (auto _ : state) {
    LargeDataTableBenchmarkObject tested({8, 8, 8}, 100, txn_length_, update_select_ratio_, &block_store_,
                                         &buffer_pool_, &generator_, true);
    StartGC(tested.GetTimestampManager(), tested.GetTxnManager());
    uint64_t elapsed_ms;
    {
      common::ScopedTimer<std::chrono::milliseconds> timer(&elapsed_ms);
      tested.SimulateOltp(num_txns_, num_concurrent_txns_);
    }
    lag_count += EndGC();
    state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);
  }
  state.SetItemsProcessed(state.iterations() * num_txns_ - lag_count);
}

BENCHMARK_REGISTER_F(GarbageCollectorBenchmark, UnlinkTime)->Unit(benchmark::kMillisecond)->UseManualTime()->MinTime(1);
BENCHMARK_REGISTER_F(GarbageCollectorBenchmark, ReclaimTime)
    ->Unit(benchmark::kMillisecond)
    ->UseManualTime()
    ->MinTime(1);
BENCHMARK_REGISTER_F(GarbageCollectorBenchmark, HighContention)
    ->Unit(benchmark::kMillisecond)
    ->UseManualTime()
    ->MinTime(2);
}  // namespace noisepage
