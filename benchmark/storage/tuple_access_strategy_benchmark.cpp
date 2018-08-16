#include <vector>

#include "benchmark/benchmark.h"
#include "common/typedefs.h"
#include "storage/storage_util.h"
#include "storage/tuple_access_strategy.h"
#include "util/storage_test_util.h"
#include "util/multi_threaded_test_util.h"
#include "util/storage_benchmark_util.h"

namespace terrier {

// This benchmark simulates a key-value store inserting a large number of tuples. This provides a good baseline and
// reference to other fast data structures (indexes) to compare against. We are interested in the TAS' raw
// performance, so the tuple's contents are intentionally left garbage and we don't verify correctness. That's the job
// of the Google Tests.

class TupleAccessStrategyBenchmark : public benchmark::Fixture {
 public:
  void SetUp(const benchmark::State &state) final {
    // generate a random redo ProjectedRow to Insert
    redo_buffer_ = new byte[redo_size_];
    redo_ = storage::ProjectedRow::InitializeProjectedRow(redo_buffer_, all_col_ids_, layout_);
    StorageTestUtil::PopulateRandomRow(redo_, layout_, 0, &generator_);
  }
  void TearDown(const benchmark::State &state) final {
    delete[] redo_buffer_;
  }

  // Workload
  const uint32_t num_threads_ = 8;

  // Test infrastructure
  std::default_random_engine generator_;
  storage::BlockStore block_store_{1};

  // Tuple layout_
  const uint16_t num_columns_ = 2;
  const uint8_t column_size_ = 8;
  const storage::BlockLayout layout_{num_columns_, {column_size_, column_size_}};

  // Tuple properties
  const std::vector<uint16_t> all_col_ids_{StorageTestUtil::ProjectionListAllColumns(layout_)};
  const uint32_t redo_size_ = storage::ProjectedRow::Size(layout_, all_col_ids_);

  storage::RawBlock *raw_block_;
  // Insert buffer pointers
  byte *redo_buffer_;
  storage::ProjectedRow *redo_;
};

// Insert the maximum number of tuples into a Block in a single thread
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(TupleAccessStrategyBenchmark, SimpleInsert)(benchmark::State &state) {
  storage::TupleAccessStrategy tested(layout_);

  // NOLINTNEXTLINE
  for (auto _ : state) {
    // Get the Block, zero it, and initialize
    raw_block_ = block_store_.Get();
    PELOTON_MEMSET(raw_block_, 0, sizeof(storage::RawBlock));
    tested.InitializeRawBlock(raw_block_, layout_version_t(0));

    for (uint32_t j = 0; j < layout_.num_slots_; j++) {
      storage::TupleSlot slot;
      tested.Allocate(raw_block_, &slot);
      TupleAccessStrategyBenchmarkUtil::InsertTuple(*redo_,
                                                    &tested,
                                                    layout_,
                                                    slot);
    }
    block_store_.Release(raw_block_);
  }

  state.SetItemsProcessed(state.iterations() * layout_.num_slots_);
}

// Insert the maximum number of tuples into a Block concurrently
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(TupleAccessStrategyBenchmark, ConcurrentInsert)(benchmark::State &state) {
  storage::TupleAccessStrategy tested(layout_);

  // NOLINTNEXTLINE
  for (auto _ : state) {
    // Get the Block, zero it, and initialize
    raw_block_ = block_store_.Get();
    PELOTON_MEMSET(raw_block_, 0, sizeof(storage::RawBlock));
    tested.InitializeRawBlock(raw_block_, layout_version_t(0));

    auto workload = [&](uint32_t id) {
      for (uint32_t j = 0; j < layout_.num_slots_ / num_threads_; j++){
        storage::TupleSlot slot;
        tested.Allocate(raw_block_, &slot);
        TupleAccessStrategyBenchmarkUtil::InsertTuple(*redo_,
                                                      &tested,
                                                      layout_,
                                                      slot);
      }
    };

    MultiThreadedTestUtil::RunThreadsUntilFinish(num_threads_, workload);
    block_store_.Release(raw_block_);
  }

  state.SetItemsProcessed(state.iterations() * layout_.num_slots_);
}

BENCHMARK_REGISTER_F(TupleAccessStrategyBenchmark, SimpleInsert)
    ->Repetitions(10)
    ->Unit(benchmark::kMillisecond)
    ->UseRealTime();

BENCHMARK_REGISTER_F(TupleAccessStrategyBenchmark, ConcurrentInsert)
    ->Repetitions(10)
    ->Unit(benchmark::kMillisecond)
    ->UseRealTime();

}  // namespace terrier
