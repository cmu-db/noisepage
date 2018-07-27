#include "benchmark/benchmark.h"
#include "common/test_util.h"
#include "storage/tuple_access_strategy_test_util.h"

namespace terrier {

// Corresponds to TEST_F(TupleAccessStrategyTests, SimpleInsertTest)
static void BM_SimpleInsert(benchmark::State &state) {
  storage::RawBlock *raw_block_ = nullptr;
  storage::BlockStore block_store_{1};

  const uint32_t repeat = 1;
  std::default_random_engine generator;

  storage::BlockLayout layout(8, {8, 8, 8, 8, 8, 8, 8, 8});
  std::unordered_map<storage::TupleSlot, testutil::FakeRawTuple> tuples;

  while (state.KeepRunning()) {

    for (uint32_t i = 0; i < repeat; i++) {
      raw_block_ = block_store_.Get();
      PELOTON_MEMSET(raw_block_, 0, sizeof(storage::RawBlock));
      storage::InitializeRawBlock(raw_block_, layout, 0);
      storage::TupleAccessStrategy tested(layout);

      for (uint32_t j = 0; j < layout.num_slots_; j++)
        testutil::TryInsertFakeTuple(layout,
                                     tested,
                                     raw_block_,
                                     tuples,
                                     generator);

      tuples.clear();
      block_store_.Release(raw_block_);

    }

  }

  state.SetBytesProcessed(state.iterations() * repeat * 64);
}

BENCHMARK(BM_SimpleInsert)
->Repetitions(3)->
Unit(benchmark::kMillisecond);

}