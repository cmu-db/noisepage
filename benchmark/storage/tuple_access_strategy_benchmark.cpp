#include "benchmark/benchmark.h"
#include "util/multi_threaded_test_util.h"
#include "util/tuple_access_strategy_test_util.h"

namespace terrier {

// Roughly corresponds to TEST_F(TupleAccessStrategyTests, SimpleInsertTest)
static void BM_SimpleInsert(benchmark::State &state) {

  // Get a BlockStore and then RawBlock to use for inserting into
  storage::RawBlock *raw_block_ = nullptr;
  storage::BlockStore block_store_{1};

  // Number of times to repeat the Initialize and Insert loop
  const uint32_t repeat = 3;
  std::default_random_engine generator;

  // Tuple layout
  uint16_t num_columns = 8;
  uint8_t column_size = 8;
  storage::BlockLayout layout(num_columns,
                              {column_size, column_size, column_size, column_size, column_size, column_size,
                               column_size, column_size});
  storage::TupleAccessStrategy tested(layout);
  std::unordered_map<storage::TupleSlot, FakeRawTuple> tuples;

  while (state.KeepRunning()) {

    for (uint32_t i = 0; i < repeat; i++) {
      // Get the Block, zero it, and initialize
      raw_block_ = block_store_.Get();
      PELOTON_MEMSET(raw_block_, 0, sizeof(storage::RawBlock));
      tested.InitializeRawBlock(raw_block_, layout_version_t(0));

      // Insert the maximum number of tuples into this Block
      for (uint32_t j = 0; j < layout.num_slots_; j++)
        TupleAccessStrategyTestUtil::TryInsertFakeTuple(layout,
                                                        tested,
                                                        raw_block_,
                                                        tuples,
                                                        generator);

      tuples.clear();
      block_store_.Release(raw_block_);

    }

  }

  // We want to approximate the amount of data processed so Google Benchmark can print stats for us
  // We'll say it 2x RawBlock because we zero it, and then populate it. This is likely an underestimation
  size_t bytes_per_repeat = 2 * sizeof(storage::RawBlock);
  state.SetBytesProcessed(state.iterations() * repeat * bytes_per_repeat);
}

BENCHMARK(BM_SimpleInsert)
->Repetitions(3)->
    Unit(benchmark::kMillisecond);

}