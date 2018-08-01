#include <unordered_map>

#include <benchmark/benchmark.h>
#include "common/test_util.h"
#include "common/typedefs.h"
#include "storage/storage_utils.h"
#include "storage/tuple_access_strategy.h"
#include "util/storage_test_util.h"

//TODO(Matt): nuke this once we have a valid benchmark to refer to

namespace terrier {

// This does NOT return a sensible tuple in general. This is just some filler
// to write into the storage layer and is devoid of meaning outside of this class.
// This is distinct from a ProjectedRow and should only be used to exercise TupleAccessStrategy's ability to access
// RawBlocks.
// ProjectedRow has additional checks and invariants only meaningful at the DataTable level
struct FakeRawTuple {
  template<typename Random>
  FakeRawTuple(const storage::BlockLayout &layout, Random &generator)
      : layout_(layout), attr_offsets_(), contents_(new byte[layout.tuple_size_]) {
    uint32_t pos = 0;
    for (uint16_t col = 0; col < layout.num_cols_; col++) {
      attr_offsets_.push_back(pos);
      pos += layout.attr_sizes_[col];
    }
    testutil::FillWithRandomBytes(layout.tuple_size_, contents_, generator);
  }

  ~FakeRawTuple() { delete[] contents_; }

  // Since all fields we store in pages are equal to or shorter than 8 bytes,
  // we can do equality checks on uint64_t always.
  // 0 return for non-primary key indexes should be treated as null.
  uint64_t Attribute(const storage::BlockLayout &layout, uint16_t col) {
    return storage::ReadBytes(layout.attr_sizes_[col],
                              contents_ + attr_offsets_[col]);
  }

  const storage::BlockLayout &layout_;
  std::vector<uint32_t> attr_offsets_;
  byte *contents_;
};

// Write the given fake tuple into a block using the given access strategy,
// at the specified offset
void InsertTuple(FakeRawTuple &tuple, storage::TupleAccessStrategy &tested, const storage::BlockLayout &layout,
                 storage::TupleSlot slot) {
  for (uint16_t col = 0; col < layout.num_cols_; col++) {
    uint64_t col_val = tuple.Attribute(layout, col);
    if (col_val != 0 || col == PRESENCE_COLUMN_ID)
      storage::WriteBytes(layout.attr_sizes_[col],
                          tuple.Attribute(layout, col),
                          tested.AccessForceNotNull(slot, col));
    else
      tested.SetNull(slot, col);
    // Otherwise leave the field as null.
  }
}

// Check that the written tuple is the same as the expected one
void CheckTupleEqual(FakeRawTuple &expected, storage::TupleAccessStrategy &tested, const storage::BlockLayout &layout,
                     storage::TupleSlot slot) {
  for (uint16_t col = 0; col < layout.num_cols_; col++) {
    uint64_t expected_col = expected.Attribute(layout, col);
    // 0 return for non-primary key indexes should be treated as null.
    bool null = (expected_col == 0) && (col != PRESENCE_COLUMN_ID);
    byte *col_slot = tested.AccessWithNullCheck(slot, col);
    if (!null) {
      EXPECT_TRUE(col_slot != nullptr);
      EXPECT_EQ(expected.Attribute(layout, col),
                storage::ReadBytes(layout.attr_sizes_[col], col_slot));
    } else {
      EXPECT_TRUE(col_slot == nullptr);
    }
  }
}

// Using the given random generator, attempts to allocate a slot and write a
// random tuple into it. The slot and the tuple are logged in the given map.
// Checks are performed to make sure the insertion is sensible.
template<typename Random>
std::pair<const storage::TupleSlot, FakeRawTuple> &TryInsertFakeTuple(
    const storage::BlockLayout &layout, storage::TupleAccessStrategy &tested, storage::RawBlock *block,
    std::unordered_map<storage::TupleSlot, FakeRawTuple> &tuples, Random &generator) {
  storage::TupleSlot slot;
  // There should always be enough slots.
  EXPECT_TRUE(tested.Allocate(block, slot));
  EXPECT_TRUE(tested.ColumnNullBitmap(block,
                                      PRESENCE_COLUMN_ID)->Test(slot.GetOffset()));

  // Construct a random tuple and associate it with the tuple slot
  auto result =
      tuples.emplace(std::piecewise_construct, std::forward_as_tuple(slot), std::forward_as_tuple(layout, generator));
  // The tuple slot is not something that is already in use.
  EXPECT_TRUE(result.second);
  InsertTuple(result.first->second, tested, layout, slot);
  return *(result.first);
}

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
      storage::InitializeRawBlock(raw_block_, layout, VALUE_OF(layout_version_t, 0u));

      // Insert the maximum number of tuples into this Block
      for (uint32_t j = 0; j < layout.num_slots_; j++)
        TryInsertFakeTuple(layout,
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