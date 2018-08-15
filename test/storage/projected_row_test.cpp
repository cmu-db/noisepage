#include <unordered_map>
#include <utility>
#include <vector>
#include "common/object_pool.h"
#include "storage/data_table.h"
#include "storage/storage_util.h"
#include "util/storage_test_util.h"
#include "storage/storage_defs.h"

namespace terrier {

class ProjectedRowTestObject {
 public:
  ~ProjectedRowTestObject() {
    for (auto entry : loose_pointers_) {
      delete[] entry;
    }
  }

  std::vector<byte *> loose_pointers_;
};

struct ProjectedRowTests : public ::testing::Test {
  std::default_random_engine generator_;
  std::uniform_real_distribution<double> null_ratio_{0.0, 1.0};
};

// Generates a random table layout and a random table layout. Coin flip bias for an attribute being null and set the
// value bytes for null attributes to be 0. Then, compare the addresses (and values for null attribute) returned by
// the access methods. Repeats for num_iterations.
// NOLINTNEXTLINE
TEST_F(ProjectedRowTests, Nulls) {
  const uint32_t num_iterations = 10;
  const uint16_t max_columns = 100;

  for (uint32_t iteration = 0; iteration < num_iterations; ++iteration) {
    ProjectedRowTestObject test_obj;

    // get a random table layout
    storage::BlockLayout layout = StorageTestUtil::RandomLayout(max_columns, &generator_);

    // generate a random projectedRow
    std::vector<uint16_t> update_col_ids = StorageTestUtil::ProjectionListAllColumns(layout);
    auto *update_buffer = new byte[storage::ProjectedRow::Size(layout, update_col_ids)];
    storage::ProjectedRow *update =
        storage::ProjectedRow::InitializeProjectedRow(update_buffer, update_col_ids, layout);
    StorageTestUtil::PopulateRandomRow(update, layout, null_ratio_(generator_), &generator_);
    test_obj.loose_pointers_.push_back(update_buffer);


    // generator a binary vector and set nulls according to binary vector. For null attributes, we set value to be 0.
    std::bernoulli_distribution coin(null_ratio_(generator_));
    std::vector<bool> null_cols(update->NumColumns());
    for (uint16_t  i = 0; i < update->NumColumns(); i++) {
      null_cols[i] = coin(generator_);
      if (null_cols[i]) {
        byte * val_ptr = update->AccessForceNotNull(i);
        storage::StorageUtil::WriteBytes(layout.attr_sizes_[i+1], 0, val_ptr);
        update->SetNull(i);
    } else {
        update->SetNotNull(i);
      }
    }
    // check correctness
    for (uint16_t i = 0; i < update->NumColumns(); i++) {
      byte * addr = update->AccessWithNullCheck(i);
      if (null_cols[i]) {
        EXPECT_EQ(addr, nullptr);
        byte * val_ptr = update->AccessForceNotNull(i);
        EXPECT_EQ(0, storage::StorageUtil::ReadBytes(layout.attr_sizes_[i+1], val_ptr));
      } else {
        EXPECT_FALSE(addr == nullptr);
      }
    }
  }
}

// This test generates randomized block layouts, and checks its layout to ensure
// that the header, the column bitmaps, and the columns don't overlap, and don't
// go out of page boundary. (In other words, memory safe.)
// NOLINTNEXTLINE
TEST_F(ProjectedRowTests, MemorySafety){
  const uint32_t num_iterations = 500;
  for (uint32_t iteration = 0; iteration < num_iterations; iteration++) {
    ProjectedRowTestObject test_obj;
    // get a random table layout
    storage::BlockLayout layout = StorageTestUtil::RandomLayout(MAX_COL, &generator_);

    // generate a random projectedRow
    std::vector<uint16_t> update_col_ids = StorageTestUtil::ProjectionListAllColumns(layout);
    auto *update_buffer = new byte[storage::ProjectedRow::Size(layout, update_col_ids)];
    storage::ProjectedRow *update =
        storage::ProjectedRow::InitializeProjectedRow(update_buffer, update_col_ids, layout);
    test_obj.loose_pointers_.push_back(update_buffer);

    EXPECT_EQ(layout.num_cols_ -1, update->NumColumns());
    void *lower_bound = update->ColumnIds();
    void *upper_bound = update + update->Size();
    // check the first value is in bound
    StorageTestUtil::CheckInBounds(update->AccessForceNotNull(0), lower_bound, upper_bound);
    // check the rest values memory addresses don't overlapping previous addresses.
    for (uint16_t col = 1; col < update->NumColumns(); col++) {
      lower_bound = update->AccessForceNotNull(static_cast<uint16_t >(col - 1));
      upper_bound = StorageTestUtil::IncrementByBytes(update->AccessForceNotNull(static_cast<uint16_t >(col - 1)),
                                                      layout.attr_sizes_[col]);
      // check if the value address is in bound
      StorageTestUtil::CheckNotInBounds(update->AccessForceNotNull(col), lower_bound, upper_bound);
    }
  }
}
}  // namespace terrier
