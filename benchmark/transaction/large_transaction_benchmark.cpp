#include "benchmark/benchmark.h"
#include "common/scoped_timer.h"
#include "storage/garbage_collector.h"
#include "util/transaction_benchmark_util.h"

namespace terrier {

class LargeTransactionBenchmark : public benchmark::Fixture {
 public:

  const std::vector<uint8_t> attr_sizes = {8, 8, 8, 8, 8, 8, 8, 8, 8, 8};
  const uint32_t initial_table_size = 1000000;
  const uint32_t num_txns = 100000;
  storage::BlockStore block_store_{1000, 1000};
  storage::RecordBufferSegmentPool buffer_pool_{1000000, 1000000};
  std::default_random_engine generator_;
  const uint32_t num_concurrent_txns_ = 6;
};

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(LargeTransactionBenchmark, Throughput)(benchmark::State &state) {
  const uint32_t txn_length = 1;
  const std::vector<double> update_select_ratio = {1.0, 0.0};
  // NOLINTNEXTLINE
  for (auto _ : state) {
    LargeTransactionBenchmarkObject tested(attr_sizes, initial_table_size, txn_length, update_select_ratio, &block_store_,
                                      &buffer_pool_, &generator_, false);
    uint64_t elapsed_ms;
    {
      common::ScopedTimer timer(&elapsed_ms);
      tested.SimulateOltp(num_txns, num_concurrent_txns_);
    }
    state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);
  }
  state.SetItemsProcessed(state.iterations() * num_txns);
}

BENCHMARK_REGISTER_F(LargeTransactionBenchmark, Throughput)
    ->Unit(benchmark::kMillisecond)
    ->UseManualTime()
    ->MinTime(1);
}  // namespace terrier