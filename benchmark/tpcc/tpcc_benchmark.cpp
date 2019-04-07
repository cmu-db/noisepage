#include <random>
#include <vector>
#include "benchmark/benchmark.h"
#include "common/scoped_timer.h"
#include "storage/garbage_collector.h"
#include "storage/storage_defs.h"
#include "tpcc/builder.h"
#include "tpcc/database.h"
#include "transaction/transaction_manager.h"

namespace terrier {

class TPCCBenchmark : public benchmark::Fixture {
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
  const uint64_t blockstore_size_limit_ = 1000;
  const uint64_t blockstore_reuse_limit_ = 1000;
  const uint64_t buffersegment_size_limit_ = 1000000;
  const uint64_t buffersegment_reuse_limit_ = 1000000;
  storage::BlockStore block_store_{blockstore_size_limit_, blockstore_reuse_limit_};
  storage::RecordBufferSegmentPool buffer_pool_{buffersegment_size_limit_, buffersegment_reuse_limit_};
  std::default_random_engine generator_;

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

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(TPCCBenchmark, Basic)(benchmark::State &state) {
  // NOLINTNEXTLINE
  for (auto _ : state) {
    transaction::TransactionManager txn_manager(&buffer_pool_, true, LOGGING_DISABLED);
    StartGC(&txn_manager);
    auto tpcc_builder = tpcc::Builder(&txn_manager, &block_store_, &generator_);
    tpcc::Database *tpcc_db = nullptr;
    uint64_t elapsed_ms;
    {
      common::ScopedTimer timer(&elapsed_ms);
      tpcc_db = tpcc_builder.Build();
    }
    state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);
    EndGC();
    delete tpcc_db;
  }
  state.SetItemsProcessed(state.iterations());
}

BENCHMARK_REGISTER_F(TPCCBenchmark, Basic)->Unit(benchmark::kMillisecond)->UseManualTime();
// BENCHMARK_REGISTER_F(TPCCBenchmark, Basic)->Unit(benchmark::kMillisecond);
}  // namespace terrier
