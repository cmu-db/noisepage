#include "benchmark/benchmark.h"
#include "common/typedefs.h"
#include "storage/data_table.h"
#include "storage/storage_util.h"
#include "util/storage_test_util.h"
#include "util/multi_threaded_test_util.h"
#include "util/storage_benchmark_util.h"
#include "transaction/transaction_context.h"
#include "transaction/transaction_manager.h"

namespace terrier {

class DataTableBenchmark : public benchmark::Fixture {
 public:
  void SetUp(const benchmark::State &state) final {
    redo_buffer_ = new byte[redo_size_];

    // generate a random redo ProjectedRow to Insert
    redo_ = storage::ProjectedRow::InitializeProjectedRow(redo_buffer_, all_col_ids_, layout_);
    StorageTestUtil::PopulateRandomRow(redo_, layout_, 0, &generator_);
  }
  void TearDown(const benchmark::State &state) final {
    delete[] redo_buffer_;
  }

  // Workload
  const uint32_t num_inserts_ = 10000000;
  const uint32_t num_threads_ = 8;

  // Test infrastructure
  std::default_random_engine generator_;
  storage::BlockStore block_store_{1000};
  common::ObjectPool<transaction::UndoBufferSegment> buffer_pool_{num_inserts_};

  // Tuple layout
  const uint16_t num_columns_ = 2;
  const uint8_t column_size_ = 8;
  const storage::BlockLayout layout_{num_columns_, {column_size_, column_size_}};

  // Tuple properties
  const std::vector<uint16_t> all_col_ids_{StorageTestUtil::ProjectionListAllColumns(layout_)};
  const uint32_t redo_size_ = storage::ProjectedRow::Size(layout_, all_col_ids_);
  const uint32_t undo_size_ = storage::DeltaRecord::Size(layout_, all_col_ids_);

  // Insert buffer pointers
  byte *redo_buffer_;
  storage::ProjectedRow *redo_;
};

// Test raw DataTable insert time. Generate a fixed layout allocate undo and redo buffers, and then reuse them to
// repeatedly insert the same garbage tuple over and over into the DataTable to test throughput.
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(DataTableBenchmark, SimpleInsert)(benchmark::State &state) {
  // NOLINTNEXTLINE
  for (auto _ : state) {
    storage::DataTable table(&block_store_, layout_);
    transaction::TransactionContext txn(timestamp_t(0), timestamp_t(0), &buffer_pool_);
    for (uint32_t i = 0; i < num_inserts_; ++i) {
      table.Insert(&txn, *redo_);
    }
  }

  state.SetItemsProcessed(state.iterations() * num_inserts_);
}


// Test raw DataTable insert time concurrently. Generate a fixed layout allocate undo and redo buffers, and then reuse
// them to repeatedly insert the same garbage tuple over and over into the DataTable to test throughput.
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(DataTableBenchmark, ConcurrentInsert)(benchmark::State &state) {
  // NOLINTNEXTLINE
  for (auto _ : state) {
    storage::DataTable table(&block_store_, layout_);
    auto workload = [&](uint32_t id) {
      transaction::TransactionContext txn(timestamp_t(0), timestamp_t(0), &buffer_pool_);
      for (uint32_t i = 0; i < num_inserts_ / num_threads_; i++)
        table.Insert(&txn, *redo_);
    };
    MultiThreadedTestUtil::RunThreadsUntilFinish(num_threads_, workload);
  }

  state.SetItemsProcessed(state.iterations() * num_inserts_);
}

BENCHMARK_REGISTER_F(DataTableBenchmark, SimpleInsert)
    ->Repetitions(10)
    ->Unit(benchmark::kMillisecond)
    ->UseRealTime();

BENCHMARK_REGISTER_F(DataTableBenchmark, ConcurrentInsert)
    ->Repetitions(10)
    ->Unit(benchmark::kMillisecond)
    ->UseRealTime();

}  // namespace terrier
