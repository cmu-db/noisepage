#include "storage/tuple_access_strategy_test_util.h"
#include <cstdio>
namespace terrier {
struct TupleAccessStrategyTests : public ::testing::Test {
  block_id_t id_;
  RawBlock *raw_block_ = nullptr;
  ObjectPool<RawBlock> pool_{1};
  storage::BlockStore block_store_{pool_};
 protected:
  void SetUp() override {
    auto result = block_store_.NewBlock();
    id_ = result.first;
    raw_block_ = result.second;
  }

  void TearDown() override {
    block_store_.UnsafeDeallocate(id_);
  }
};

// Tests that we can set things to null and the access strategy returns
// nullptr for null fields.
TEST_F(TupleAccessStrategyTests, NullTest) {
  const int32_t repeat = 100;
  std::default_random_engine generator;
  for (int32_t i = 0; i < repeat; i++) {
    storage::BlockLayout layout = testutil::RandomLayout(generator);
    PELOTON_MEMSET(raw_block_, 0, sizeof(RawBlock));
    storage::InitializeRawBlock(raw_block_, layout, id_);
    storage::TupleAccessStrategy tested(layout);

    uint32_t offset;
    EXPECT_TRUE(tested.Allocate(raw_block_, offset));
    std::vector<bool> nulls(layout.num_cols_);
    std::bernoulli_distribution coin(0.5);
    // primary key always not null
    nulls[0] = false;
    // Randomly set some columns to be not null
    for (uint16_t col = 1; col < layout.num_cols_; col++) {
      if (coin(generator))
        nulls[col] = true;
      else {
        nulls[col] = false;
        tested.AccessForceNotNull(raw_block_, col, offset);
      }
    }

    for (uint16_t col = 0; col < layout.num_cols_; col++) {
      // Either the field is null and the access returns nullptr,
      // or the field is not null and the access ptr is not null
      EXPECT_TRUE(
          (tested.AccessWithNullCheck(raw_block_, col, offset) != nullptr)
              ^ nulls[col]);
    }

    // Flip non-null columns to null should result in returning of nullptr.
    for (uint16_t col = 1; col < layout.num_cols_; col++) {
      if (!nulls[col]) tested.SetNull(raw_block_, col, offset);
      EXPECT_TRUE(tested.AccessWithNullCheck(raw_block_, col, offset) == nullptr);
    }
  }
}

// Tests that we can allocate a tuple slot, write things into the slot and
// get them out.
TEST_F(TupleAccessStrategyTests, SimpleCorrectnessTest) {
  const int32_t repeat = 100;
  std::default_random_engine generator;
  for (int32_t i = 0; i < repeat; i++) {
    storage::BlockLayout layout = testutil::RandomLayout(generator);
    PELOTON_MEMSET(raw_block_, 0, sizeof(RawBlock));
    storage::InitializeRawBlock(raw_block_, layout, id_);
    storage::TupleAccessStrategy tested(layout);

    uint32_t offset;
    EXPECT_TRUE(tested.Allocate(raw_block_, offset));
    EXPECT_TRUE(tested.ColumnNullBitmap(raw_block_, PRIMARY_KEY_OFFSET)->Test(offset));
    testutil::FakeRawTuple fake_tuple = testutil::RandomTuple(layout, generator);
    testutil::InsertTuple(fake_tuple, tested, layout, raw_block_, offset);
    testutil::CheckTupleEqual(fake_tuple, tested, layout, raw_block_, offset);
  }
}

// This test generates randomized block layouts, and checks its layout to ensure
// that the header, the column bitmaps, and the columns don't overlap, and don't
// go out of page boundary. (In other words, memory safe.)
TEST_F(TupleAccessStrategyTests, MemorySafetyTest) {
  const uint32_t repeat = 500;
  std::default_random_engine generator;
  for (uint32_t i = 0; i < repeat; i++) {
    storage::BlockLayout layout = testutil::RandomLayout(generator);
    // here we don't need to 0-initialize the block because we only
    // test layout, not the content.
    storage::InitializeRawBlock(raw_block_, layout, id_);
    storage::TupleAccessStrategy tested(layout);

    // Skip header
    void *lower_bound = tested.ColumnNullBitmap(raw_block_, 0);
    void *upper_bound = raw_block_ + sizeof(RawBlock);
    for (uint16_t col = 0; col < layout.num_cols_; col++) {
      // This test should be robust against any future paddings, since
      // we are checking for non-overlapping ranges and not hard-coded
      // boundaries.
      testutil::CheckInBounds(tested.ColumnNullBitmap(raw_block_, col),
                              lower_bound,
                              upper_bound);
      lower_bound =
          testutil::IncrementByBytes(tested.ColumnNullBitmap(raw_block_, col),
                                     BitmapSize(layout.num_slots_));

      testutil::CheckInBounds(tested.ColumnStart(raw_block_, col),
                              lower_bound,
                              upper_bound);

      lower_bound =
          testutil::IncrementByBytes(tested.ColumnStart(raw_block_, col),
                                     layout.num_slots_
                                         * layout.attr_sizes_[col]);
    }
    // check that the last column does not go out of the block
    uint32_t last_column_size =
        layout.num_slots_ * layout.attr_sizes_[layout.num_cols_ - 1];
    testutil::CheckInBounds(testutil::IncrementByBytes(lower_bound,
                                                       last_column_size),
                            lower_bound,
                            upper_bound);
  }
}
}