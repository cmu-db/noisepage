#include <unordered_map>
#include <utility>
#include <vector>

#include "common/object_pool.h"
#include "storage/data_table.h"
#include "storage/storage_defs.h"
#include "storage/storage_util.h"
#include "test_util/storage_test_util.h"
#include "test_util/test_harness.h"

namespace noisepage {

struct ProjectedRowTests : public TerrierTest {
  std::default_random_engine generator_;
  std::uniform_real_distribution<double> null_ratio_{0.0, 1.0};
};

// Generates a random table layout and a random table layout. Coin flip bias for an attribute being null. Then, compare
// the addresses (and values for null attribute) returned by the access methods. Repeats for num_iterations.
// NOLINTNEXTLINE
TEST_F(ProjectedRowTests, Nulls) {
  const uint32_t num_iterations = 10;

  for (uint32_t iteration = 0; iteration < num_iterations; ++iteration) {
    // get a random table layout
    storage::BlockLayout layout = StorageTestUtil::RandomLayoutNoVarlen(common::Constants::MAX_COL, &generator_);

    // generate a random projectedRow
    std::vector<storage::col_id_t> update_col_ids = StorageTestUtil::ProjectionListAllColumns(layout);
    storage::ProjectedRowInitializer initializer = storage::ProjectedRowInitializer::Create(layout, update_col_ids);
    auto *update_buffer = common::AllocationUtil::AllocateAligned(initializer.ProjectedRowSize());
    storage::ProjectedRow *update = initializer.InitializeRow(update_buffer);
    StorageTestUtil::PopulateRandomRow(update, layout, null_ratio_(generator_), &generator_);

    // generator a binary vector and set nulls according to binary vector. For null attributes, we set value to be 0.
    std::bernoulli_distribution coin(null_ratio_(generator_));
    std::vector<bool> nulls(update->NumColumns());
    for (uint16_t i = 0; i < update->NumColumns(); i++) {
      nulls[i] = coin(generator_);
      if (nulls[i]) {
        update->SetNull(i);
      } else {
        update->SetNotNull(i);
      }
    }
    // check correctness
    for (uint16_t i = 0; i < update->NumColumns(); i++) {
      byte *addr = update->AccessWithNullCheck(i);
      if (nulls[i]) {
        EXPECT_EQ(addr, nullptr);
      } else {
        EXPECT_FALSE(addr == nullptr);
      }
    }
    delete[] update_buffer;
  }
}

// This tests checks that the copy function works as intended
// NOLINTNEXTLINE
TEST_F(ProjectedRowTests, CopyProjectedRowLayout) {
  const uint32_t num_iterations = 50;
  for (uint32_t iteration = 0; iteration < num_iterations; iteration++) {
    // get a random table layout
    storage::BlockLayout layout = StorageTestUtil::RandomLayoutNoVarlen(common::Constants::MAX_COL, &generator_);

    // generate a random projectedRow
    std::vector<storage::col_id_t> all_col_ids = StorageTestUtil::ProjectionListAllColumns(layout);
    storage::ProjectedRowInitializer initializer = storage::ProjectedRowInitializer::Create(layout, all_col_ids);
    auto *buffer = common::AllocationUtil::AllocateAligned(initializer.ProjectedRowSize());
    storage::ProjectedRow *row = initializer.InitializeRow(buffer);

    auto *copy = common::AllocationUtil::AllocateAligned(row->Size());
    auto *copied_row = storage::ProjectedRow::CopyProjectedRowLayout(copy, *row);

    EXPECT_EQ(copied_row->NumColumns(), row->NumColumns());
    for (uint16_t i = 0; i < row->NumColumns(); i++) {
      EXPECT_EQ(row->ColumnIds()[i], copied_row->ColumnIds()[i]);
      uintptr_t offset = reinterpret_cast<uintptr_t>(row->AccessForceNotNull(i)) - reinterpret_cast<uintptr_t>(row);
      uintptr_t copied_offset =
          reinterpret_cast<uintptr_t>(copied_row->AccessForceNotNull(i)) - reinterpret_cast<uintptr_t>(copied_row);
      EXPECT_EQ(offset, copied_offset);
    }
    delete[] buffer;
    delete[] copy;
  }
}

// This test generates randomized block layouts, and checks its layout to ensure
// that the header, the column bitmaps, and the columns don't overlap, and don't
// go out of page boundary. (In other words, memory safe.)
// NOLINTNEXTLINE
TEST_F(ProjectedRowTests, MemorySafety) {
  const uint32_t num_iterations = 50;
  for (uint32_t iteration = 0; iteration < num_iterations; iteration++) {
    // get a random table layout
    storage::BlockLayout layout = StorageTestUtil::RandomLayoutNoVarlen(common::Constants::MAX_COL, &generator_);

    // generate a random projectedRow
    std::vector<storage::col_id_t> all_col_ids = StorageTestUtil::ProjectionListAllColumns(layout);
    storage::ProjectedRowInitializer initializer = storage::ProjectedRowInitializer::Create(layout, all_col_ids);
    auto *buffer = common::AllocationUtil::AllocateAligned(initializer.ProjectedRowSize());
    storage::ProjectedRow *row = initializer.InitializeRow(buffer);

    EXPECT_EQ(layout.NumColumns() - storage::NUM_RESERVED_COLUMNS, row->NumColumns());
    void *upper_bound = reinterpret_cast<byte *>(row) + row->Size();
    // check the rest values memory addresses don't overlapping previous addresses.
    for (uint16_t i = 1; i < row->NumColumns(); i++) {
      void *lower_bound = StorageTestUtil::IncrementByBytes(row->AccessForceNotNull(static_cast<uint16_t>(i - 1)),
                                                            layout.AttrSize(row->ColumnIds()[i - 1]));
      // Check that the previous address is within allocated bounds
      StorageTestUtil::CheckInBounds(lower_bound, row, upper_bound);
      // check if the value address is in bound
      StorageTestUtil::CheckInBounds(row->AccessForceNotNull(i), lower_bound, upper_bound);
    }
    delete[] buffer;
  }
}

// This test checks that all the fields within projected row is aligned
// NOLINTNEXTLINE
TEST_F(ProjectedRowTests, Alignment) {
  const uint32_t num_iterations = 50;
  for (uint32_t iteration = 0; iteration < num_iterations; iteration++) {
    // get a random table layout
    storage::BlockLayout layout = StorageTestUtil::RandomLayoutNoVarlen(common::Constants::MAX_COL, &generator_);

    // generate a random projectedRow
    std::vector<storage::col_id_t> all_col_ids = StorageTestUtil::ProjectionListAllColumns(layout);
    storage::ProjectedRowInitializer initializer = storage::ProjectedRowInitializer::Create(layout, all_col_ids);
    auto *buffer = common::AllocationUtil::AllocateAligned(initializer.ProjectedRowSize());
    storage::ProjectedRow *row = initializer.InitializeRow(buffer);
    for (uint16_t i = 0; i < row->NumColumns(); i++)
      StorageTestUtil::CheckAlignment(row->AccessForceNotNull(i), layout.AttrSize(row->ColumnIds()[i]));
    delete[] buffer;
  }
}
}  // namespace noisepage
