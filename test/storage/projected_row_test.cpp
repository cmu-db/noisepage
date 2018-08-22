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

struct ProjectedRowTests : public TerrierTest {
  std::default_random_engine generator_;
  std::uniform_real_distribution<double> null_ratio_{0.0, 1.0};
  std::vector<byte *> loose_pointers_;

 protected:
  void SetUp() override {
    TerrierTest::SetUp();
  }

  void TearDown() override {
    for (auto entry : loose_pointers_) {
      delete[] entry;
    }
    TerrierTest::TearDown();
  }
};

// Generates a random table layout and a random table layout. Coin flip bias for an attribute being null and set the
// value bytes for null attributes to be 0. Then, compare the addresses (and values for null attribute) returned by
// the access methods. Repeats for num_iterations.
// NOLINTNEXTLINE
TEST_F(ProjectedRowTests, Nulls) {
  const uint32_t num_iterations = 10;

  for (uint32_t iteration = 0; iteration < num_iterations; ++iteration) {
    // get a random table layout
    storage::BlockLayout layout = StorageTestUtil::RandomLayout(common::Constants::MAX_COL, &generator_);

    // generate a random projectedRow
    std::vector<uint16_t> update_col_ids = StorageTestUtil::ProjectionListAllColumns(layout);
    storage::ProjectedRowInitializer initializer(layout, update_col_ids);
    auto *update_buffer = StorageTestUtil::AllocateAligned(initializer.ProjectedRowSize());
    storage::ProjectedRow *update = initializer.InitializeProjectedRow(update_buffer);
    StorageTestUtil::PopulateRandomRow(update, layout, null_ratio_(generator_), &generator_);
    loose_pointers_.push_back(update_buffer);


    // generator a binary vector and set nulls according to binary vector. For null attributes, we set value to be 0.
    std::bernoulli_distribution coin(null_ratio_(generator_));
    std::vector<bool> null_cols(update->NumColumns());
    for (uint16_t i = 0; i < update->NumColumns(); i++) {
      null_cols[i] = coin(generator_);
      if (null_cols[i]) {
        byte *val_ptr = update->AccessForceNotNull(i);
        storage::StorageUtil::WriteBytes(layout.attr_sizes_[i + 1], 0, val_ptr);
        update->SetNull(i);
      } else {
        update->SetNotNull(i);
      }
    }
    // check correctness
    for (uint16_t i = 0; i < update->NumColumns(); i++) {
      byte *addr = update->AccessWithNullCheck(i);
      if (null_cols[i]) {
        EXPECT_EQ(addr, nullptr);
        byte *val_ptr = update->AccessForceNotNull(i);
        EXPECT_EQ(0, storage::StorageUtil::ReadBytes(layout.attr_sizes_[i + 1], val_ptr));
      } else {
        EXPECT_FALSE(addr == nullptr);
      }
    }
  }
}

// NOLINTNEXTLINE
// This tests checks that the copy function works as intended
TEST_F(ProjectedRowTests, CopyProjectedRowLayout) {
  const uint32_t num_iterations = 500;
  for (uint32_t iteration = 0; iteration < num_iterations; iteration++) {
    // get a random table layout
    storage::BlockLayout layout = StorageTestUtil::RandomLayout(common::Constants::MAX_COL, &generator_);

    // generate a random projectedRow
    std::vector<uint16_t> all_col_ids = StorageTestUtil::ProjectionListAllColumns(layout);
    storage::ProjectedRowInitializer initializer(layout, all_col_ids);
    auto *buffer = StorageTestUtil::AllocateAligned(initializer.ProjectedRowSize());
    storage::ProjectedRow *row = initializer.InitializeProjectedRow(buffer);
    loose_pointers_.push_back(buffer);

    auto *copy = StorageTestUtil::AllocateAligned(row->Size());
    auto *copied_row = storage::ProjectedRow::CopyProjectedRowLayout(copy, *row);

    EXPECT_EQ(copied_row->NumColumns(), row->NumColumns());
    for (uint16_t i = 0; i < row->NumColumns(); i++) {
      EXPECT_EQ(row->ColumnIds()[i], copied_row->ColumnIds()[i]);
      uintptr_t offset = reinterpret_cast<uintptr_t>(row->AccessForceNotNull(i)) - reinterpret_cast<uintptr_t>(row);
      uintptr_t copied_offset = reinterpret_cast<uintptr_t>(copied_row->AccessForceNotNull(i))
          - reinterpret_cast<uintptr_t>(copied_row);
      EXPECT_EQ(offset, copied_offset);
    }
  }
}

// This test generates randomized block layouts, and checks its layout to ensure
// that the header, the column bitmaps, and the columns don't overlap, and don't
// go out of page boundary. (In other words, memory safe.)
// NOLINTNEXTLINE
TEST_F(ProjectedRowTests, MemorySafety) {
  const uint32_t num_iterations = 500;
  for (uint32_t iteration = 0; iteration < num_iterations; iteration++) {
    // get a random table layout
    storage::BlockLayout layout = StorageTestUtil::RandomLayout(common::Constants::MAX_COL, &generator_);

    // generate a random projectedRow
    std::vector<uint16_t> all_col_ids = StorageTestUtil::ProjectionListAllColumns(layout);
    storage::ProjectedRowInitializer initializer(layout, all_col_ids);
    auto *buffer = StorageTestUtil::AllocateAligned(initializer.ProjectedRowSize());
    storage::ProjectedRow *row = initializer.InitializeProjectedRow(buffer);
    loose_pointers_.push_back(buffer);

    EXPECT_EQ(layout.num_cols_ - 1, row->NumColumns());
    void *upper_bound = reinterpret_cast<byte *>(row) + row->Size();
    // check the rest values memory addresses don't overlapping previous addresses.
    for (uint16_t i = 1; i < row->NumColumns(); i++) {
      void *lower_bound = StorageTestUtil::IncrementByBytes(row->AccessForceNotNull(static_cast<uint16_t>(i - 1)),
                                                            layout.attr_sizes_[row->ColumnIds()[i - 1]]);
      // Check that the previous address is within allocated bounds
      StorageTestUtil::CheckInBounds(lower_bound, row, upper_bound);
      // check if the value address is in bound
      StorageTestUtil::CheckInBounds(row->AccessForceNotNull(i), lower_bound, upper_bound);
    }
  }
}

// NOLINTNEXTLINE
// This test checks that all the fields within projected row is aligned
TEST_F(ProjectedRowTests, Alignment) {
  const uint32_t num_iterations = 500;
  for (uint32_t iteration = 0; iteration < num_iterations; iteration++) {
    // get a random table layout
    storage::BlockLayout layout = StorageTestUtil::RandomLayout(common::Constants::MAX_COL, &generator_);

    // generate a random projectedRow
    std::vector<uint16_t> all_col_ids = StorageTestUtil::ProjectionListAllColumns(layout);
    storage::ProjectedRowInitializer initializer(layout, all_col_ids);
    auto *buffer = StorageTestUtil::AllocateAligned(initializer.ProjectedRowSize());
    storage::ProjectedRow *row = initializer.InitializeProjectedRow(buffer);
    loose_pointers_.push_back(buffer);
    for (uint16_t i = 0; i < row->NumColumns(); i++)
      StorageTestUtil::CheckAlignment(row->AccessForceNotNull(i),
                                      layout.attr_sizes_[row->ColumnIds()[i]]);
  }
}
}  // namespace terrier
