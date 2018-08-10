#include <unordered_map>
#include <vector>

#include "benchmark/benchmark.h"
#include "common/typedefs.h"
#include "storage/storage_util.h"
#include "storage/tuple_access_strategy.h"
#include "util/storage_test_util.h"
#include "util/multi_threaded_test_util.h"
#include "util/storage_benchmark_util.h"

namespace terrier {

// Roughly corresponds to TEST_F(TupleAccessStrategyTests, SimpleInsert)
// NOLINTNEXTLINE
static void BM_SimpleInsert(benchmark::State &state) {
  // Get a BlockStore and then RawBlock to use for inserting into
  storage::RawBlock *raw_block_ = nullptr;
  storage::BlockStore block_store_{1};

  std::default_random_engine generator;

  // Tuple layout
  uint16_t num_columns = 2;
  uint8_t column_size = 8;
  storage::BlockLayout layout(num_columns, {column_size, column_size});
  storage::TupleAccessStrategy tested(layout);

  // Generate a random redo ProjectedRow to insert
  std::vector<uint16_t> all_col_ids_{StorageTestUtil::ProjectionListAllColumns(layout)};
  uint32_t row_size_ = storage::ProjectedRow::Size(layout, all_col_ids_);
  byte *row_buffer = new byte[row_size_];
  storage::ProjectedRow *row = storage::ProjectedRow::InitializeProjectedRow(row_buffer, all_col_ids_, layout);
  StorageTestUtil::PopulateRandomRow(row, layout, 0, &generator);


  while (state.KeepRunning()) {
    // Get the Block, zero it, and initialize
    raw_block_ = block_store_.Get();
    PELOTON_MEMSET(raw_block_, 0, sizeof(storage::RawBlock));
    tested.InitializeRawBlock(raw_block_, layout_version_t(0));

    // Insert the maximum number of tuples into this Block
    for (uint32_t j = 0; j < layout.num_slots_; j++) {
      storage::TupleSlot slot;
      tested.Allocate(raw_block_, &slot);
      TupleAccessStrategyBenchmarkUtil::InsertTuple(*row,
                                                    &tested,
                                                    layout,
                                                    slot);
    }
    block_store_.Release(raw_block_);
  }
  // We want to approximate the amount of data processed so Google Benchmark can print stats for us
  // We'll say it 2x RawBlock because we zero it, and then populate it. This is likely an underestimation
  size_t bytes_per_repeat = 2 * sizeof(storage::RawBlock);
  state.SetBytesProcessed(state.iterations() * bytes_per_repeat);
}

// Roughly corresponds to TEST_F(TupleAccessStrategyTests, ConcurrentInsert)
// NOLINTNEXTLINE
static void BM_ConcurrentInsert(benchmark::State &state) {
  // Get a BlockStore and then RawBlock to use for inserting into
  storage::RawBlock *raw_block_ = nullptr;
  storage::BlockStore block_store_{1};

  std::default_random_engine generator;

  // Tuple layout
  uint16_t num_columns = 2;
  uint8_t column_size = 8;
  storage::BlockLayout layout(num_columns, {column_size, column_size});
  storage::TupleAccessStrategy tested(layout);

  // generate a random redo ProjectedRow to Insert
  std::vector<uint16_t> all_col_ids_{StorageTestUtil::ProjectionListAllColumns(layout)};
  uint32_t row_size_ = storage::ProjectedRow::Size(layout, all_col_ids_);
  byte *row_buffer = new byte[row_size_];
  storage::ProjectedRow *row = storage::ProjectedRow::InitializeProjectedRow(row_buffer, all_col_ids_, layout);
  StorageTestUtil::PopulateRandomRow(row, layout, 0, &generator);

  const uint32_t num_threads = 8;

  while (state.KeepRunning()) {
    // Get the Block, zero it, and initialize
    raw_block_ = block_store_.Get();
    PELOTON_MEMSET(raw_block_, 0, sizeof(storage::RawBlock));
    tested.InitializeRawBlock(raw_block_, layout_version_t(0));

    auto workload = [&](uint32_t id) {
      for (uint32_t j = 0; j < layout.num_slots_ / num_threads; j++){
        storage::TupleSlot slot;
        tested.Allocate(raw_block_, &slot);
        TupleAccessStrategyBenchmarkUtil::InsertTuple(*row,
                                                      &tested,
                                                      layout,
                                                      slot);
      }
    };

    MultiThreadedTestUtil::RunThreadsUntilFinish(num_threads, workload);
    block_store_.Release(raw_block_);
  }
  // We want to approximate the amount of data processed so Google Benchmark can print stats for us
  // We'll say it 2x RawBlock because we zero it, and then populate it. This is likely an underestimation
  size_t bytes_per_repeat = 2 * sizeof(storage::RawBlock);
  state.SetBytesProcessed(state.iterations() * bytes_per_repeat);
}

BENCHMARK(BM_SimpleInsert)
    ->Repetitions(10)
    ->Unit(benchmark::kMillisecond)
    ->UseRealTime();

BENCHMARK(BM_ConcurrentInsert)
    ->Repetitions(10)
    ->Unit(benchmark::kMillisecond)
    ->UseRealTime();

}  // namespace terrier
