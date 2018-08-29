#include <vector>

#include "benchmark/benchmark.h"
#include "common/typedefs.h"
#include "storage/data_table.h"
#include "storage/storage_util.h"
#include "transaction/transaction_context.h"
#include "transaction/transaction_manager.h"
#include "util/storage_benchmark_util.h"
#include "util/storage_test_util.h"
#include "util/test_thread_pool.h"

namespace terrier {

// This benchmark simulates a key-value store inserting a large number of tuples. This provides a good baseline and
// reference to other fast data structures (indexes) to compare against. We are interested in the DataTable's raw
// performance, so the tuple's contents are intentionally left garbage and we don't verify correctness. That's the job
// of the Google Tests.

class DataTableBenchmark : public benchmark::Fixture {
 public:
  void SetUp(const benchmark::State &state) final {
    // generate a random redo ProjectedRow to Insert
    redo_buffer_ = common::AllocationUtil::AllocateAligned(initializer_.ProjectedRowSize());
    redo_ = initializer_.InitializeRow(redo_buffer_);
    StorageTestUtil::PopulateRandomRow(redo_, layout_, 0, &generator_);
  }
  void TearDown(const benchmark::State &state) final { delete[] redo_buffer_; }

  // Tuple layout
  const uint16_t num_columns_ = 2;
  const uint8_t column_size_ = 8;
  const storage::BlockLayout layout_{num_columns_, {column_size_, column_size_}};

  // Tuple properties
  const storage::ProjectedRowInitializer initializer_{layout_, StorageTestUtil::ProjectionListAllColumns(layout_)};

  // Workload
  const uint32_t num_inserts_ = 10000000;
  const uint32_t num_threads_ = TestThreadPool::HardwareConcurrency();
  const uint64_t bp_reuse_limit_ = 10000000;

  // Test infrastructure
  std::default_random_engine generator_;
  storage::BlockStore block_store_{1000, 1000};
  common::ObjectPool<storage::BufferSegment> buffer_pool_{num_inserts_, bp_reuse_limit_};

  // Insert buffer pointers
  byte *redo_buffer_;
  storage::ProjectedRow *redo_;
};

// Insert the num_inserts_ of tuples into a DataTable in a single thread
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(DataTableBenchmark, SimpleInsert)(benchmark::State &state) {
  // NOLINTNEXTLINE
  for (auto _ : state) {
    storage::DataTable table(&block_store_, layout_);
    // We can use dummy timestamps here since we're not invoking concurrency control
    transaction::TransactionContext txn(timestamp_t(0), timestamp_t(0), &buffer_pool_);
    for (uint32_t i = 0; i < num_inserts_; ++i) {
      table.Insert(&txn, *redo_);
    }
  }

  state.SetItemsProcessed(state.iterations() * num_inserts_);
}

// Insert the num_inserts_ of tuples into a DataTable concurrently
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(DataTableBenchmark, ConcurrentInsert)(benchmark::State &state) {
  TestThreadPool thread_pool;
  // NOLINTNEXTLINE
  for (auto _ : state) {
    storage::DataTable table(&block_store_, layout_);
    auto workload = [&](uint32_t id) {
      // We can use dummy timestamps here since we're not invoking concurrency control
      transaction::TransactionContext txn(timestamp_t(0), timestamp_t(0), &buffer_pool_);
      for (uint32_t i = 0; i < num_inserts_ / num_threads_; i++) table.Insert(&txn, *redo_);
    };
    thread_pool.RunThreadsUntilFinish(num_threads_, workload);
  }

  state.SetItemsProcessed(state.iterations() * num_inserts_);
}

BENCHMARK_REGISTER_F(DataTableBenchmark, SimpleInsert)->Unit(benchmark::kMillisecond)->UseRealTime();

BENCHMARK_REGISTER_F(DataTableBenchmark, ConcurrentInsert)->Unit(benchmark::kMillisecond)->UseRealTime();

}  // namespace terrier
