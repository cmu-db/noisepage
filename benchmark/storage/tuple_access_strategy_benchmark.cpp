#include <cstring>
#include <vector>

#include "benchmark/benchmark.h"
#include "common/strong_typedef.h"
#include "storage/storage_util.h"
#include "storage/tuple_access_strategy.h"
#include "test_util/multithread_test_util.h"
#include "test_util/storage_test_util.h"

namespace noisepage {

// This benchmark simulates a key-value store inserting a large number of tuples. This provides a good baseline and
// reference to other fast data structures (indexes) to compare against. We are interested in the TAS' raw
// performance, so the tuple's contents are intentionally left garbage and we don't verify correctness. That's the job
// of the Google Tests.

class TupleAccessStrategyBenchmark : public benchmark::Fixture {
 public:
  void SetUp(const benchmark::State &state) final {
    // generate a random redo ProjectedRow to Insert
    redo_buffer_ = common::AllocationUtil::AllocateAligned(initializer_.ProjectedRowSize());
    redo_ = initializer_.InitializeRow(redo_buffer_);
    StorageTestUtil::PopulateRandomRow(redo_, layout_, 0, &generator_);
  }

  void TearDown(const benchmark::State &state) final { delete[] redo_buffer_; }

  // Tuple layout_
  const uint8_t column_size_ = 8;
  const storage::BlockLayout layout_{{column_size_, column_size_, column_size_}};

  // Tuple properties
  const storage::ProjectedRowInitializer initializer_ =
      storage::ProjectedRowInitializer::Create(layout_, StorageTestUtil::ProjectionListAllColumns(layout_));

  // Workload
  const uint32_t num_inserts_ = 10000000;
  const uint32_t num_blocks_ = num_inserts_ / layout_.NumSlots();
  const uint64_t block_store_reuse_limit_ = num_blocks_;

  // Test infrastructure
  std::default_random_engine generator_;
  storage::BlockStore block_store_{num_blocks_, block_store_reuse_limit_};

  std::vector<storage::RawBlock *> raw_blocks_;
  // Insert buffer pointers
  byte *redo_buffer_;
  storage::ProjectedRow *redo_;
};

// Insert the num_inserts_ of tuples into Blocks in a single thread
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(TupleAccessStrategyBenchmark, SimpleInsert)(benchmark::State &state) {
  storage::TupleAccessStrategy tested(layout_);

  // NOLINTNEXTLINE
  for (auto _ : state) {
    for (uint32_t i = 0; i < num_blocks_; i++) {
      // Get a Block, zero it, and initialize
      storage::RawBlock *raw_block = block_store_.Get();
      raw_blocks_.emplace_back(raw_block);
      // Recast raw_block as a -Wclass-memaccess workaround
      std::memset(static_cast<void *>(raw_block), 0, sizeof(storage::RawBlock));
      tested.InitializeRawBlock(nullptr, raw_block, storage::layout_version_t(0));
      for (uint32_t j = 0; j < layout_.NumSlots(); j++) {
        storage::TupleSlot slot;
        tested.Allocate(raw_block, &slot);
        StorageTestUtil::InsertTuple(*redo_, tested, layout_, slot);
      }
    }
    // return all of the used blocks to the BlockStore
    for (uint32_t i = 0; i < num_blocks_; i++) {
      block_store_.Release(raw_blocks_[i]);
    }
  }

  state.SetItemsProcessed(state.iterations() * layout_.NumSlots() * num_blocks_);
}

BENCHMARK_REGISTER_F(TupleAccessStrategyBenchmark, SimpleInsert)->Unit(benchmark::kMillisecond);

}  // namespace noisepage
