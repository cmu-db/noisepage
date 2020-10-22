#include <memory>
#include <vector>

#include "benchmark/benchmark.h"
#include "benchmark_util/benchmark_config.h"
#include "common/scoped_timer.h"
#include "storage/data_table.h"
#include "storage/storage_util.h"
#include "test_util/multithread_test_util.h"
#include "test_util/storage_test_util.h"
#include "transaction/transaction_context.h"

namespace terrier {

/**
 * This benchmark measures how fast the
 */
class SlotIteratorBenchmark : public benchmark::Fixture {
 public:
  void SetUp(const benchmark::State &state) final {
    // generate a random redo ProjectedRow to Insert
    redo_buffer_ = common::AllocationUtil::AllocateAligned(initializer_.ProjectedRowSize());
    redo_ = initializer_.InitializeRow(redo_buffer_);
    StorageTestUtil::PopulateRandomRow(redo_, layout_, 0, &generator_);

    // generate a ProjectedRow buffer to Read
    read_buffer_ = common::AllocationUtil::AllocateAligned(initializer_.ProjectedRowSize());
    read_ = initializer_.InitializeRow(read_buffer_);

    // generate a vector of ProjectedRow buffers for concurrent reads
    for (uint32_t i = 0; i < BenchmarkConfig::num_threads; ++i) {
      // Create read buffer
      byte *read_buffer = common::AllocationUtil::AllocateAligned(initializer_.ProjectedRowSize());
      storage::ProjectedRow *read = initializer_.InitializeRow(read_buffer);
      read_buffers_.emplace_back(read_buffer);
      reads_.emplace_back(read);
    }
  }

  void TearDown(const benchmark::State &state) final {
    delete[] redo_buffer_;
    delete[] read_buffer_;
    for (uint32_t i = 0; i < BenchmarkConfig::num_threads; ++i) delete[] read_buffers_[i];
    // google benchmark might run benchmark several iterations. We need to clear vectors.
    read_buffers_.clear();
    reads_.clear();
  }

  // Tuple layout
  const uint8_t column_size_ = 8;
  const storage::BlockLayout layout_{{column_size_, column_size_, column_size_}};

  // Tuple properties
  const storage::ProjectedRowInitializer initializer_ =
      storage::ProjectedRowInitializer::Create(layout_, StorageTestUtil::ProjectionListAllColumns(layout_));

  // Workload
  const uint32_t num_reads_ = 10000000;
  const uint64_t buffer_pool_reuse_limit_ = 10000000;

  // Test infrastructure
  std::default_random_engine generator_;
  storage::BlockStore block_store_{1000, 1000};
  storage::RecordBufferSegmentPool buffer_pool_{num_reads_, buffer_pool_reuse_limit_};

  // Insert buffer pointers
  byte *redo_buffer_;
  storage::ProjectedRow *redo_;

  // Read buffer pointers;
  byte *read_buffer_;
  storage::ProjectedRow *read_;

  // Read buffers pointers for concurrent reads
  std::vector<byte *> read_buffers_;
  std::vector<storage::ProjectedRow *> reads_;
};

// Iterate the num_reads_ of tuples in the sequential  order from a DataTable concurrently
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(SlotIteratorBenchmark, ConcurrentSlotIterators)(benchmark::State &state) {
  storage::DataTable read_table(common::ManagedPointer<storage::BlockStore>(&block_store_), layout_,
                                storage::layout_version_t(0));

  // populate read_table_ by inserting tuples
  // We can use dummy timestamps here since we're not invoking concurrency control
  transaction::TransactionContext txn(transaction::timestamp_t(0), transaction::timestamp_t(0),
                                      common::ManagedPointer(&buffer_pool_), DISABLED);
  for (uint32_t i = 0; i < num_reads_; ++i) {
    read_table.Insert(common::ManagedPointer(&txn), *redo_);
  }

  auto workload = [&]() {
    auto it = read_table.begin();
    uint32_t num_reads = 0;
    while (it != read_table.end()) {
      num_reads++;
      it++;
    }
    EXPECT_EQ(num_reads, num_reads_);
  };

  common::WorkerPool thread_pool(BenchmarkConfig::num_threads, {});
  thread_pool.Startup();

  // NOLINTNEXTLINE
  for (auto _ : state) {
    uint64_t elapsed_ms;
    {
      common::ScopedTimer<std::chrono::milliseconds> timer(&elapsed_ms);
      for (uint32_t j = 0; j < BenchmarkConfig::num_threads; j++) {
        thread_pool.SubmitTask([&workload] { workload(); });
      }
      thread_pool.WaitUntilAllFinished();
    }
    state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);
  }
  state.SetItemsProcessed(state.iterations() * num_reads_ * BenchmarkConfig::num_threads);
}

// Iterate the num_reads_ of tuples in the sequential  order from a DataTable concurrently
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(SlotIteratorBenchmark, ConcurrentSlotIteratorsReads)(benchmark::State &state) {
  storage::DataTable read_table(common::ManagedPointer<storage::BlockStore>(&block_store_), layout_,
                                storage::layout_version_t(0));

  // populate read_table_ by inserting tuples
  // We can use dummy timestamps here since we're not invoking concurrency control
  transaction::TransactionContext txn(transaction::timestamp_t(0), transaction::timestamp_t(0),
                                      common::ManagedPointer(&buffer_pool_), DISABLED);
  for (uint32_t i = 0; i < num_reads_; ++i) {
    read_table.Insert(common::ManagedPointer(&txn), *redo_);
  }

  std::vector<std::vector<storage::TupleSlot>> reads(BenchmarkConfig::num_threads);

  for (auto &i : reads) i.resize(num_reads_);

  auto workload = [&](const uint32_t worker_id) {
    auto it = read_table.begin();
    uint32_t num_reads = 0;
    while (it != read_table.end()) {
      reads[worker_id][num_reads++] = *it++;
    }
    EXPECT_EQ(num_reads, num_reads_);
  };

  common::WorkerPool thread_pool(BenchmarkConfig::num_threads, {});
  thread_pool.Startup();

  // NOLINTNEXTLINE
  for (auto _ : state) {
    uint64_t elapsed_ms;
    {
      common::ScopedTimer<std::chrono::milliseconds> timer(&elapsed_ms);
      for (uint32_t j = 0; j < BenchmarkConfig::num_threads; j++) {
        thread_pool.SubmitTask([&workload, j] { workload(j); });
      }
      thread_pool.WaitUntilAllFinished();
    }
    state.SetIterationTime(static_cast<double>(elapsed_ms) / 1000.0);
  }
  state.SetItemsProcessed(state.iterations() * num_reads_ * BenchmarkConfig::num_threads);
}

// ----------------------------------------------------------------------------
// BENCHMARK REGISTRATION
// ----------------------------------------------------------------------------
// clang-format off
BENCHMARK_REGISTER_F(SlotIteratorBenchmark, ConcurrentSlotIterators)
->Unit(benchmark::kMillisecond)
    ->UseRealTime()
    ->UseManualTime();
BENCHMARK_REGISTER_F(SlotIteratorBenchmark, ConcurrentSlotIteratorsReads)
->Unit(benchmark::kMillisecond)
    ->UseRealTime()
    ->UseManualTime();
// clang-format on

}  // namespace terrier
