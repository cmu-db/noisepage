#include <unordered_map>
#include <utility>
#include <vector>
#include "common/object_pool.h"
#include "storage/data_table.h"
#include "storage/storage_util.h"
#include "util/storage_test_util.h"
#include "storage/storage_defs.h"
#include "util/test_harness.h"

namespace terrier {

struct StorageUtilTests : public ::terrier::TerrierTest {
  std::default_random_engine generator_;
  std::uniform_real_distribution<double> null_ratio_{0.0, 1.0};

  storage::RawBlock *raw_block_ = nullptr;
  storage::BlockStore block_store_{1};

  std::vector<byte *> loose_pointers_;

 protected:
  void SetUp() override {
    TerrierTest::SetUp();
    raw_block_ = block_store_.Get();
  }

  void TearDown() override {
    block_store_.Release(raw_block_);

    for (auto &entry : loose_pointers_) {
      delete[] entry;
    }
    TerrierTest::TearDown();
  }
};


// Write a value to a position, read from the same position and compare results. Repeats for num_iterations.
// NOLINTNEXTLINE
TEST_F(StorageUtilTests, ReadWriteBytes) {
  uint32_t num_iterations = 500;
  for (uint32_t iteration = 0; iteration < num_iterations; ++iteration) {
    // generate a random val
    std::vector<uint8_t> valid_sizes{1, 2, 4, 8};
    std::uniform_int_distribution<uint8_t> idx(0, static_cast<uint8_t>(valid_sizes.size() - 1));
    uint8_t attr_size = valid_sizes[idx(generator_)];
    uint64_t val = 0;
    StorageTestUtil::FillWithRandomBytes(attr_size, reinterpret_cast<byte *>(&val), &generator_);

    // Write and read again to see if we get the same value;
    byte pos[8];
    storage::StorageUtil::WriteBytes(attr_size, val, pos);
    EXPECT_EQ(val, storage::StorageUtil::ReadBytes(attr_size, pos));
  }
}

// Generate a random projected row layout, copy a pointer location into a projected row, read it back from projected
// row and compare results for each column. Repeats for num_iterations.
// NOLINTNEXTLINE
TEST_F(StorageUtilTests, CopyToProjectedRow) {
  uint32_t num_iterations = 500;
  for (uint32_t iteration = 0; iteration < num_iterations; ++iteration) {
    // get a random table layout
    storage::BlockLayout layout = StorageTestUtil::RandomLayout(MAX_COL, &generator_);

    // generate a random projectedRow
    std::vector<uint16_t> update_col_ids = StorageTestUtil::ProjectionListAllColumns(layout);
    auto *row_buffer = new byte[storage::ProjectedRow::Size(layout, update_col_ids)];
    storage::ProjectedRow *row =
        storage::ProjectedRow::InitializeProjectedRow(row_buffer, update_col_ids, layout);
    loose_pointers_.push_back(row_buffer);

    std::bernoulli_distribution null_dist(null_ratio_(generator_));
    for (uint16_t col = 0; col < row->NumColumns(); ++col) {
      uint8_t attr_size = layout.attr_sizes_[static_cast<uint16_t >(col+1)];
      byte *from = nullptr;
      bool is_null = null_dist(generator_);
      if (!is_null) {
        // generate a random val
        from = new byte[attr_size];
        loose_pointers_.push_back(from);
        StorageTestUtil::FillWithRandomBytes(attr_size, from, &generator_);
      }
      storage::StorageUtil::CopyWithNullCheck(from, row, attr_size, col);

      if (is_null) {
        EXPECT_EQ(row->AccessWithNullCheck(col), nullptr);
      } else {
        EXPECT_EQ(storage::StorageUtil::ReadBytes(attr_size, from),
            storage::StorageUtil::ReadBytes(attr_size, row->AccessWithNullCheck(col)));
      }
    }
  }
}


// Generate a layout and get a tuple slot, copy a pointer location into the tuple slot, read it back and
// compare results for each column. Repeats for num_iterations.
// NOLINTNEXTLINE
TEST_F(StorageUtilTests, CopyToTupleSlot) {
  uint32_t num_iterations = 500;
  for (uint32_t iteration = 0; iteration < num_iterations; ++iteration) {
    storage::BlockLayout layout = StorageTestUtil::RandomLayout(MAX_COL, &generator_);
    storage::TupleAccessStrategy tested(layout);
    PELOTON_MEMSET(raw_block_, 0, sizeof(storage::RawBlock));
    tested.InitializeRawBlock(raw_block_, layout_version_t(0));

    storage::TupleSlot slot;
    EXPECT_TRUE(tested.Allocate(raw_block_, &slot));

    std::bernoulli_distribution null_dist(null_ratio_(generator_));
    for (uint16_t col = 0; col < layout.num_cols_; ++col) {
      uint8_t attr_size = layout.attr_sizes_[col];
      byte *from = nullptr;
      bool is_null = null_dist(generator_);
      if (!is_null) {
        // generate a random val
        from = new byte[attr_size];
        loose_pointers_.push_back(from);
        StorageTestUtil::FillWithRandomBytes(attr_size, from, &generator_);
      }
      storage::StorageUtil::CopyWithNullCheck(from, tested, slot, col);

      if (is_null) {
        EXPECT_EQ(tested.AccessWithNullCheck(slot, col), nullptr);
      } else {
        EXPECT_EQ(storage::StorageUtil::ReadBytes(attr_size, from),
                  storage::StorageUtil::ReadBytes(attr_size, tested.AccessWithNullCheck(slot, col)));
      }
    }
  }
}


// Generate a random populated projected row (delta), copy the delta into a projected row, and compare them.
// Repeats for num_iterations.
// NOLINTNEXTLINE
TEST_F(StorageUtilTests, ApplyDelta) {
  uint32_t num_iterations = 500;
  for (uint32_t iteration = 0; iteration < num_iterations; ++iteration) {
    // get a random table layout
    storage::BlockLayout layout = StorageTestUtil::RandomLayout(MAX_COL, &generator_);

    // generate a random projected row
    std::vector<uint16_t> col_ids = StorageTestUtil::ProjectionListRandomColumns(layout, &generator_);
    std::unordered_map<uint16_t, uint16_t> col_to_index;
    for (uint32_t i = 0; i < col_ids.size(); ++i) {
      col_to_index.insert(std::make_pair(col_ids[i], i));
    }
    auto *delta_buffer = new byte[storage::ProjectedRow::Size(layout, col_ids)];
    storage::ProjectedRow *delta =
        storage::ProjectedRow::InitializeProjectedRow(delta_buffer, col_ids, layout);
    StorageTestUtil::PopulateRandomRow(delta, layout, null_ratio_(generator_), &generator_);
    loose_pointers_.push_back(delta_buffer);

    // generate a new projected row
    auto *row_buffer = new byte[storage::ProjectedRow::Size(layout, col_ids)];
    storage::ProjectedRow *row =
        storage::ProjectedRow::InitializeProjectedRow(delta_buffer, col_ids, layout);
    loose_pointers_.push_back(row_buffer);

    storage::StorageUtil::ApplyDelta(layout, *delta, row, col_to_index);

    EXPECT_TRUE(StorageTestUtil::ProjectionListEqual(layout, delta, row));
  }
}

}  // namespace terrier
