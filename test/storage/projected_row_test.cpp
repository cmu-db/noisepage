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

// Generates a random table layout and coin flip bias for an attribute being null, inserts num_inserts random tuples
// into an empty DataTable. Then, Selects the inserted TupleSlots and compares the results to the original inserted
// random tuple. Repeats for num_iterations.
// NOLINTNEXTLINE
TEST_F(ProjectedRowTests, Nulls) {
  const uint32_t num_iterations = 10;
  const uint16_t max_columns = 10;

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
}  // namespace terrier
