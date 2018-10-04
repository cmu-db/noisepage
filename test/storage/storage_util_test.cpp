#include "storage/storage_util.h"
#include <algorithm>
#include <iostream>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>
#include "common/object_pool.h"
#include "storage/data_table.h"
#include "storage/storage_defs.h"
#include "util/catalog_test_util.h"
#include "util/storage_test_util.h"
#include "util/test_harness.h"

namespace terrier {

struct StorageUtilTests : public TerrierTest {
  std::default_random_engine generator_;
  std::uniform_real_distribution<double> null_ratio_{0.0, 1.0};

  storage::RawBlock *raw_block_ = nullptr;
  storage::BlockStore block_store_{1, 1};

  const uint32_t num_iterations_ = 100;

 protected:
  void SetUp() override {
    TerrierTest::SetUp();
    raw_block_ = block_store_.Get();
  }

  void TearDown() override {
    block_store_.Release(raw_block_);
    TerrierTest::TearDown();
  }
};

// Write a value to a position, read from the same position and compare results. Repeats for num_iterations.
// NOLINTNEXTLINE
TEST_F(StorageUtilTests, ReadWriteBytes) {
  for (uint32_t iteration = 0; iteration < num_iterations_; ++iteration) {
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
  for (uint32_t iteration = 0; iteration < num_iterations_; ++iteration) {
    // get a random table layout
    storage::BlockLayout layout = StorageTestUtil::RandomLayout(common::Constants::MAX_COL, &generator_);

    // generate a random projectedRow
    std::vector<col_id_t> update_col_ids = StorageTestUtil::ProjectionListAllColumns(layout);
    storage::ProjectedRowInitializer update_initializer(layout, update_col_ids);
    auto *row_buffer = common::AllocationUtil::AllocateAligned(update_initializer.ProjectedRowSize());
    storage::ProjectedRow *row = update_initializer.InitializeRow(row_buffer);

    std::bernoulli_distribution null_dist(null_ratio_(generator_));
    for (uint16_t i = 0; i < row->NumColumns(); ++i) {
      uint8_t attr_size = layout.AttrSize(col_id_t(static_cast<uint16_t>(i + 1)));
      byte *from = nullptr;
      bool is_null = null_dist(generator_);
      if (!is_null) {
        // generate a random val
        from = new byte[attr_size];
        StorageTestUtil::FillWithRandomBytes(attr_size, from, &generator_);
      }
      storage::StorageUtil::CopyWithNullCheck(from, row, attr_size, i);

      if (is_null) {
        EXPECT_EQ(row->AccessWithNullCheck(i), nullptr);
      } else {
        EXPECT_EQ(storage::StorageUtil::ReadBytes(attr_size, from),
                  storage::StorageUtil::ReadBytes(attr_size, row->AccessWithNullCheck(i)));
        delete[] from;
      }
    }
    delete[] row_buffer;
  }
}

// Generate a layout and get a tuple slot, copy a pointer location into the tuple slot, read it back and
// compare results for each column. Repeats for num_iterations.
// NOLINTNEXTLINE
TEST_F(StorageUtilTests, CopyToTupleSlot) {
  for (uint32_t iteration = 0; iteration < num_iterations_; ++iteration) {
    storage::BlockLayout layout = StorageTestUtil::RandomLayout(common::Constants::MAX_COL, &generator_);
    storage::TupleAccessStrategy tested(layout);
    TERRIER_MEMSET(raw_block_, 0, sizeof(storage::RawBlock));
    tested.InitializeRawBlock(raw_block_, layout_version_t(0));

    storage::TupleSlot slot;
    EXPECT_TRUE(tested.Allocate(raw_block_, &slot));

    std::bernoulli_distribution null_dist(null_ratio_(generator_));
    for (uint16_t i = 0; i < layout.NumColumns(); ++i) {
      col_id_t col_id(i);
      uint8_t attr_size = layout.AttrSize(col_id);
      byte *from = nullptr;
      bool is_null = null_dist(generator_);
      if (!is_null) {
        // generate a random val
        from = new byte[attr_size];
        StorageTestUtil::FillWithRandomBytes(attr_size, from, &generator_);
      }
      storage::StorageUtil::CopyWithNullCheck(from, tested, slot, col_id);

      if (is_null) {
        EXPECT_EQ(tested.AccessWithNullCheck(slot, col_id), nullptr);
      } else {
        EXPECT_EQ(storage::StorageUtil::ReadBytes(attr_size, from),
                  storage::StorageUtil::ReadBytes(attr_size, tested.AccessWithNullCheck(slot, col_id)));
        delete[] from;
      }
    }
  }
}

// Generate a random populated projected row (delta), copy the delta into a projected row, and compare them.
// Repeats for num_iterations.
// NOLINTNEXTLINE
TEST_F(StorageUtilTests, ApplyDelta) {
  for (uint32_t iteration = 0; iteration < num_iterations_; ++iteration) {
    // get a random table layout
    storage::BlockLayout layout = StorageTestUtil::RandomLayout(common::Constants::MAX_COL, &generator_);

    // the old row
    std::vector<col_id_t> all_col_ids = StorageTestUtil::ProjectionListAllColumns(layout);
    storage::ProjectedRowInitializer initializer(layout, all_col_ids);
    auto *old_buffer = common::AllocationUtil::AllocateAligned(initializer.ProjectedRowSize());
    storage::ProjectedRow *old = initializer.InitializeRow(old_buffer);
    StorageTestUtil::PopulateRandomRow(old, layout, null_ratio_(generator_), &generator_);

    // store the values as a reference
    std::vector<std::pair<byte *, uint64_t>> copy;
    for (uint16_t i = 0; i < old->NumColumns(); ++i) {
      col_id_t col_id(i);
      byte *ptr = old->AccessWithNullCheck(i);
      if (ptr != nullptr)
        copy.emplace_back(
            std::make_pair(ptr, storage::StorageUtil::ReadBytes(layout.AttrSize(col_id + NUM_RESERVED_COLUMNS), ptr)));
      else
        copy.emplace_back(std::make_pair(ptr, 0));
    }

    // the delta change to apply
    std::vector<col_id_t> rand_col_ids = StorageTestUtil::ProjectionListRandomColumns(layout, &generator_);
    storage::ProjectedRowInitializer rand_initializer(layout, rand_col_ids);
    auto *delta_buffer = common::AllocationUtil::AllocateAligned(rand_initializer.ProjectedRowSize());
    storage::ProjectedRow *delta = rand_initializer.InitializeRow(delta_buffer);
    StorageTestUtil::PopulateRandomRow(delta, layout, null_ratio_(generator_), &generator_);

    // apply delta
    storage::StorageUtil::ApplyDelta(layout, *delta, old);
    // check changes has been applied
    for (uint16_t delta_col_offset = 0; delta_col_offset < rand_initializer.NumColumns(); ++delta_col_offset) {
      col_id_t col = rand_initializer.ColId(delta_col_offset);
      auto old_col_offset =
          static_cast<uint16_t>(!col - NUM_RESERVED_COLUMNS);  // since all columns were in the old one
      byte *delta_val_ptr = delta->AccessWithNullCheck(delta_col_offset);
      byte *old_val_ptr = old->AccessWithNullCheck(old_col_offset);
      if (delta_val_ptr == nullptr) {
        EXPECT_TRUE(old_val_ptr == nullptr);
      } else {
        // check that the change has been applied
        EXPECT_EQ(storage::StorageUtil::ReadBytes(layout.AttrSize(col), delta_val_ptr),
                  storage::StorageUtil::ReadBytes(layout.AttrSize(col), old_val_ptr));
      }
    }

    // check whether other cols have been polluted
    std::unordered_set<col_id_t> changed_cols(rand_col_ids.begin(), rand_col_ids.end());
    for (uint16_t i = 0; i < old->NumColumns(); ++i) {
      if (changed_cols.find(all_col_ids[i]) == changed_cols.end()) {
        byte *ptr = old->AccessWithNullCheck(i);
        EXPECT_EQ(ptr, copy[i].first);
        if (ptr != nullptr) {
          col_id_t col_id(static_cast<uint16_t>(i + NUM_RESERVED_COLUMNS));
          EXPECT_EQ(storage::StorageUtil::ReadBytes(layout.AttrSize(col_id), ptr), copy[i].second);
        }
      }
    }

    delete[] delta_buffer;
    delete[] old_buffer;
  }
}

// Verifies that we properly generate both a valid BlockLayout and a valid mapping from col_oid to col_id for a given
// random Schema
// NOLINTNEXTLINE
TEST_F(StorageUtilTests, BlockLayoutFromSchema) {
  for (uint32_t iteration = 0; iteration < num_iterations_; iteration++) {
    uint16_t max_columns = 1000;
    const catalog::Schema schema =
        CatalogTestUtil::RandomSchema(static_cast<uint16_t>(max_columns - NUM_RESERVED_COLUMNS), &generator_);
    const auto layout_and_col_map = storage::StorageUtil::BlockLayoutFromSchema(schema);
    const storage::BlockLayout layout = layout_and_col_map.first;
    const std::unordered_map<col_oid_t, col_id_t> column_map = layout_and_col_map.second;

    // BlockLayout should have number of columns as Schema + NUM_RESERVED_COLUMNS because Schema doesn't know anything
    // about the storage layer's reserved columns
    EXPECT_EQ(layout.NumColumns(), schema.GetColumns().size() + NUM_RESERVED_COLUMNS);
    // column_map should have number of columns as Schema + NUM_RESERVED_COLUMNS because Schema doesn't know anything
    // about the storage layer's reserved columns
    EXPECT_EQ(layout.NumColumns(), column_map.size() + NUM_RESERVED_COLUMNS);

    // Verify that the BlockLayout's columns are sorted by attribute size in descending order
    for (uint16_t i = 0; i < layout.NumColumns() - 1; i++) {
      EXPECT_GE(layout.AttrSize(col_id_t(i)), layout.AttrSize(col_id_t(static_cast<uint16_t>(i + 1))));
    }

    // Verify the contents of the column_map
    for (const auto &i : column_map) {
      const col_oid_t col_oid = i.first;
      const col_id_t col_id = i.second;

      // Column id should not map to either of the reserved columns
      EXPECT_NE(col_id, col_id_t(0));
      // Find the Column in the Schema corresponding to the current oid
      auto schema_column =
          std::find_if(schema.GetColumns().cbegin(), schema.GetColumns().cend(),
                       [&](const catalog::Schema::Column &col) -> bool { return col.GetOid() == col_oid; });
      // The attribute size in the schema should match the attribute size in the BlockLayout
      EXPECT_EQ(schema_column->GetAttrSize(), layout.AttrSize(col_id));
    }
  }
}

}  // namespace terrier
