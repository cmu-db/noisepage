#include <memory>
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

    // generate a ProjectedRow buffer to Read
    read_buffer_ = common::AllocationUtil::AllocateAligned(initializer_.ProjectedRowSize());
    read_ = initializer_.InitializeRow(read_buffer_);
  }

  void TearDown(const benchmark::State &state) final { delete[] redo_buffer_; }

  // Tuple layout
  const uint8_t column_size_ = 8;
  const storage::BlockLayout layout_{{column_size_, column_size_}};

  // Tuple properties
  const storage::ProjectedRowInitializer initializer_{layout_, StorageTestUtil::ProjectionListAllColumns(layout_)};

  // Workload
  const uint32_t num_inserts_ = 10000000;
  const uint32_t num_reads_ = 10000000;
  const uint32_t num_threads_ = TestThreadPool::HardwareConcurrency();
  const uint64_t buffer_pool_reuse_limit_ = 10000000;

  // Test infrastructure
  std::default_random_engine generator_;
  storage::BlockStore block_store_{1000, 1000};
  common::ObjectPool<storage::BufferSegment> buffer_pool_{num_inserts_, buffer_pool_reuse_limit_};

  // Insert buffer pointers
  byte *redo_buffer_;
  storage::ProjectedRow *redo_;

  // Read buffer Pointers;
  byte *read_buffer_;
  storage::ProjectedRow *read_;
};

// Insert the num_inserts_ of tuples into a DataTable in a single thread
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(DataTableBenchmark, SimpleInsert)(benchmark::State &state) {
  // NOLINTNEXTLINE
  for (auto _ : state) {
    storage::DataTable table(&block_store_, layout_, layout_version_t(0));
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
    storage::DataTable table(&block_store_, layout_, layout_version_t(0));
    auto workload = [&](uint32_t id) {
      // We can use dummy timestamps here since we're not invoking concurrency control
      transaction::TransactionContext txn(timestamp_t(0), timestamp_t(0), &buffer_pool_);
      for (uint32_t i = 0; i < num_inserts_ / num_threads_; i++) table.Insert(&txn, *redo_);
    };
    thread_pool.RunThreadsUntilFinish(num_threads_, workload);
  }

  state.SetItemsProcessed(state.iterations() * num_inserts_);
}

// Read the num_reads_ of tuples in a sequential order from a DataTable in a single thread
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(DataTableBenchmark, SequentialRead)(benchmark::State &state) {
  storage::DataTable read_table(&block_store_, layout_, layout_version_t(0));
  // Populate read_table by inserting tuples
  // We can use dummy timestamps here since we're not invoking concurrency control
  transaction::TransactionContext txn(timestamp_t(0), timestamp_t(0), &buffer_pool_);
  std::vector<storage::TupleSlot> read_order;
  for (uint32_t i = 0; i < num_reads_; ++i) {
    read_order.emplace_back(read_table.Insert(&txn, *redo_));
  }
  // NOLINTNEXTLINE
  for (auto _ : state) {
    // We can use dummy timestamps here since we're not invoking concurrency control
    transaction::TransactionContext txn(timestamp_t(0), timestamp_t(0), &buffer_pool_);
    for (uint32_t i = 0; i < num_reads_; ++i) {
      read_table.Select(&txn, read_order[i], read_);
    }
  }

  state.SetItemsProcessed(state.iterations() * num_reads_);
}

// Read the num_reads_ of tuples in a random order from a DataTable in a single thread
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(DataTableBenchmark, RandomRead)(benchmark::State &state) {
  storage::DataTable read_table(&block_store_, layout_, layout_version_t(0));
  // Populate read_table_ by inserting tuples
  // We can use dummy timestamps here since we're not invoking concurrency control
  transaction::TransactionContext txn(timestamp_t(0), timestamp_t(0), &buffer_pool_);
  std::vector<storage::TupleSlot> read_order;
  for (uint32_t i = 0; i < num_reads_; ++i) {
    read_order.emplace_back(read_table.Insert(&txn, *redo_));
  }
  // Create random reads
  std::shuffle(read_order.begin(), read_order.end(), generator_);
  // NOLINTNEXTLINE
  for (auto _ : state) {
    // We can use dummy timestamps here since we're not invoking concurrency control
    transaction::TransactionContext txn(timestamp_t(0), timestamp_t(0), &buffer_pool_);
    for (uint32_t i = 0; i < num_reads_; ++i) {
      read_table.Select(&txn, read_order[i], read_);
    }
  }

  state.SetItemsProcessed(state.iterations() * num_reads_);
}

// Read the num_reads_ of tuples in a random order from a DataTable concurrently
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(DataTableBenchmark, ConcurrentRandomRead)(benchmark::State &state) {
  TestThreadPool thread_pool;
  storage::DataTable read_table(&block_store_, layout_, layout_version_t(0));
  // populate read_table_ by inserting tuples
  // We can use dummy timestamps here since we're not invoking concurrency control
  transaction::TransactionContext txn(timestamp_t(0), timestamp_t(0), &buffer_pool_);
  std::vector<storage::TupleSlot> read_order;
  for (uint32_t i = 0; i < num_reads_; ++i) {
    read_order.emplace_back(read_table.Insert(&txn, *redo_));
  }
  // Generate random read orders and read buffer for each thread
  std::vector<std::vector<storage::TupleSlot>> read_orders;
  std::vector<byte *> read_buffers;
  std::vector<storage::ProjectedRow *> reads;

  for (uint32_t i = 0; i < num_threads_; i++) {
    // Create random reads
    std::shuffle(read_order.begin(), read_order.end(), generator_);
    read_orders.emplace_back(
        std::vector<storage::TupleSlot>(read_order.begin(), read_order.begin() + (num_reads_ / num_threads_)));
    // Create read buffer
    byte *read_buffer = common::AllocationUtil::AllocateAligned(initializer_.ProjectedRowSize());
    storage::ProjectedRow *read = initializer_.InitializeRow(read_buffer);
    read_buffers.emplace_back(read_buffer);
    reads.emplace_back(read);
  }
  // NOLINTNEXTLINE
  for (auto _ : state) {
    auto workload = [&](uint32_t id) {
      // We can use dummy timestamps here since we're not invoking concurrency control
      transaction::TransactionContext txn(timestamp_t(0), timestamp_t(0), &buffer_pool_);
      for (uint32_t i = 0; i < num_reads_ / num_threads_; i++) read_table.Select(&txn, read_orders[id][i], reads[id]);
    };
    thread_pool.RunThreadsUntilFinish(num_threads_, workload);
  }

  state.SetItemsProcessed(state.iterations() * num_reads_);
}

BENCHMARK_REGISTER_F(DataTableBenchmark, SimpleInsert)->Unit(benchmark::kMillisecond);

BENCHMARK_REGISTER_F(DataTableBenchmark, ConcurrentInsert)->Unit(benchmark::kMillisecond)->UseRealTime();

BENCHMARK_REGISTER_F(DataTableBenchmark, SequentialRead)->Unit(benchmark::kMillisecond);

BENCHMARK_REGISTER_F(DataTableBenchmark, RandomRead)->Unit(benchmark::kMillisecond);

BENCHMARK_REGISTER_F(DataTableBenchmark, ConcurrentRandomRead)->Unit(benchmark::kMillisecond)->UseRealTime();
}  // namespace terrier
