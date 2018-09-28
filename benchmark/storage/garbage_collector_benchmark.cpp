#include <utility>
#include <vector>

#include "benchmark/benchmark.h"
#include "common/scoped_timer.h"
#include "storage/garbage_collector.h"
#include "util/transaction_test_util.h"

namespace terrier {

class GarbageCollectorBenchmark : public benchmark::Fixture {
 public:
  const uint32_t txn_length = 5;
  const std::vector<double> update_select_ratio = {1, 0};
  const uint32_t num_concurrent_txns = TestThreadPool::HardwareConcurrency();
  const uint16_t max_columns = 3;
  const uint32_t initial_table_size = 100000;
  const uint32_t num_txns = 100000;
  storage::BlockStore block_store_{1000, 1000};
  storage::RecordBufferSegmentPool buffer_pool_{100000, 100000};
  std::default_random_engine generator_;
  storage::GarbageCollector *gc_ = nullptr;
};

// Create a table with 100,000 tuples, then run 100,000 txns running update statements. Then run GC and profile how long
// the unlinking stage takes for those txns
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(GarbageCollectorBenchmark, UnlinkTime)(benchmark::State &state) {
  // NOLINTNEXTLINE
  for (auto _ : state) {
    // generate our table and instantiate GC
    LargeTransactionTestObject tested(max_columns, initial_table_size, txn_length, update_select_ratio, &block_store_,
                                      &buffer_pool_, &generator_, true, false);
    gc_ = new storage::GarbageCollector(tested.GetTxnManager());

    // clean up insert txn
    gc_->PerformGarbageCollection();
    gc_->PerformGarbageCollection();

    // run all txns
    tested.SimulateOltp(num_txns, num_concurrent_txns);

    // time just the unlinking process, verify nothing deallocated
    uint64_t elapsed_ms;
    std::pair<uint32_t, uint32_t> result;
    {
      common::ScopedTimer timer(&elapsed_ms);
      result = gc_->PerformGarbageCollection();
    }
    EXPECT_EQ(result.first, 0);
    EXPECT_EQ(result.second, num_txns);

    // run another GC pass to perform deallocation, verify nothing unlinked
    result = gc_->PerformGarbageCollection();
    EXPECT_EQ(result.second, 0);

    delete gc_;

    state.SetIterationTime(elapsed_ms / 1000.0);
  }
  state.SetItemsProcessed(state.iterations() * num_txns);
}

// Create a table with 100,000 tuples, then run 100,000 txns running update statements. Then run GC and profile how long
// the deallocation stage takes for those txns
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(GarbageCollectorBenchmark, ReclaimTime)(benchmark::State &state) {
  // NOLINTNEXTLINE
  for (auto _ : state) {
    // generate our table and instantiate GC
    LargeTransactionTestObject tested(max_columns, initial_table_size, txn_length, update_select_ratio, &block_store_,
                                      &buffer_pool_, &generator_, true, false);
    gc_ = new storage::GarbageCollector(tested.GetTxnManager());

    // clean up insert txn
    gc_->PerformGarbageCollection();
    gc_->PerformGarbageCollection();

    // run all txns
    tested.SimulateOltp(num_txns, num_concurrent_txns);

    // run first pass to unlink everything, verify nothing deallocated
    std::pair<uint32_t, uint32_t> result = gc_->PerformGarbageCollection();
    EXPECT_EQ(result.first, 0);

    // time just the deallocation process, verify nothing unlinked
    uint64_t elapsed_ms;
    {
      common::ScopedTimer timer(&elapsed_ms);
      result = gc_->PerformGarbageCollection();
    }
    EXPECT_EQ(result.first, num_txns);
    EXPECT_EQ(result.second, 0);

    delete gc_;

    state.SetIterationTime(elapsed_ms / 1000.0);
  }
  state.SetItemsProcessed(state.iterations() * num_txns);
}

BENCHMARK_REGISTER_F(GarbageCollectorBenchmark, UnlinkTime)->Unit(benchmark::kMillisecond)->UseManualTime();
BENCHMARK_REGISTER_F(GarbageCollectorBenchmark, ReclaimTime)->Unit(benchmark::kMillisecond)->UseManualTime();
}  // namespace terrier
