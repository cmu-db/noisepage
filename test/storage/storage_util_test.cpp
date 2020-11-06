#include "storage/storage_util.h"

#include <cstring>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "catalog/catalog_defs.h"
#include "catalog/index_schema.h"
#include "catalog/schema.h"
#include "common/object_pool.h"
#include "parser/expression/constant_value_expression.h"
#include "storage/data_table.h"
#include "storage/storage_defs.h"
#include "test_util/storage_test_util.h"
#include "test_util/test_harness.h"

namespace noisepage {

struct StorageUtilTests : public TerrierTest {
  std::default_random_engine generator_;
  std::uniform_real_distribution<double> null_ratio_{0.0, 1.0};

  storage::RawBlock *raw_block_ = nullptr;
  storage::BlockStore block_store_{1, 1};

  const uint32_t num_iterations_ = 100;

 protected:
  void SetUp() override { raw_block_ = block_store_.Get(); }

  void TearDown() override { block_store_.Release(raw_block_); }
};

// Generate a random projected row layout, copy a pointer location into a projected row, read it back from projected
// row and compare results for each column. Repeats for num_iterations.
// NOLINTNEXTLINE
TEST_F(StorageUtilTests, CopyToProjectedRow) {
  for (uint32_t iteration = 0; iteration < num_iterations_; ++iteration) {
    // get a random table layout
    storage::BlockLayout layout = StorageTestUtil::RandomLayoutNoVarlen(common::Constants::MAX_COL, &generator_);

    // generate a random projectedRow
    std::vector<storage::col_id_t> update_col_ids = StorageTestUtil::ProjectionListAllColumns(layout);
    storage::ProjectedRowInitializer update_initializer =
        storage::ProjectedRowInitializer::Create(layout, update_col_ids);
    auto *row_buffer = common::AllocationUtil::AllocateAligned(update_initializer.ProjectedRowSize());
    storage::ProjectedRow *row = update_initializer.InitializeRow(row_buffer);

    std::bernoulli_distribution null_dist(null_ratio_(generator_));
    for (uint16_t i = 0; i < row->NumColumns(); ++i) {
      storage::col_id_t col_id(static_cast<uint16_t>(i + 1));
      uint8_t attr_size = layout.AttrSize(col_id);
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
        EXPECT_TRUE(!memcmp(from, row->AccessWithNullCheck(i), attr_size));
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
    storage::BlockLayout layout = StorageTestUtil::RandomLayoutNoVarlen(common::Constants::MAX_COL, &generator_);
    storage::TupleAccessStrategy tested(layout);
    std::memset(reinterpret_cast<void *>(raw_block_), 0, sizeof(storage::RawBlock));
    tested.InitializeRawBlock(nullptr, raw_block_, storage::layout_version_t(0));

    storage::TupleSlot slot;
    EXPECT_TRUE(tested.Allocate(raw_block_, &slot));

    std::bernoulli_distribution null_dist(null_ratio_(generator_));
    for (uint16_t i = 0; i < layout.NumColumns(); ++i) {
      storage::col_id_t col_id(i);
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
        EXPECT_TRUE(!memcmp(from, tested.AccessWithNullCheck(slot, col_id), attr_size));
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
    storage::BlockLayout layout = StorageTestUtil::RandomLayoutNoVarlen(common::Constants::MAX_COL, &generator_);

    // the old row
    std::vector<storage::col_id_t> all_col_ids = StorageTestUtil::ProjectionListAllColumns(layout);
    storage::ProjectedRowInitializer initializer = storage::ProjectedRowInitializer::Create(layout, all_col_ids);
    auto *old_buffer = common::AllocationUtil::AllocateAligned(initializer.ProjectedRowSize());
    storage::ProjectedRow *old = initializer.InitializeRow(old_buffer);
    StorageTestUtil::PopulateRandomRow(old, layout, null_ratio_(generator_), &generator_);

    // store the values as a reference
    auto *copy_bufffer = common::AllocationUtil::AllocateAligned(initializer.ProjectedRowSize());
    auto *copy = reinterpret_cast<storage::ProjectedRow *>(copy_bufffer);
    std::memcpy(reinterpret_cast<void *>(copy), old, initializer.ProjectedRowSize());

    // the delta change to apply
    std::vector<storage::col_id_t> rand_col_ids = StorageTestUtil::ProjectionListRandomColumns(layout, &generator_);
    storage::ProjectedRowInitializer rand_initializer = storage::ProjectedRowInitializer::Create(layout, rand_col_ids);
    auto *delta_buffer = common::AllocationUtil::AllocateAligned(rand_initializer.ProjectedRowSize());
    storage::ProjectedRow *delta = rand_initializer.InitializeRow(delta_buffer);
    StorageTestUtil::PopulateRandomRow(delta, layout, null_ratio_(generator_), &generator_);

    // apply delta
    storage::StorageUtil::ApplyDelta(layout, *delta, old);
    // check changes has been applied
    for (uint16_t delta_col_offset = 0; delta_col_offset < rand_initializer.NumColumns(); ++delta_col_offset) {
      storage::col_id_t col = rand_initializer.ColId(delta_col_offset);
      auto old_col_offset = static_cast<uint16_t>(
          col.UnderlyingValue() - storage::NUM_RESERVED_COLUMNS);  // since all columns were in the old one
      byte *delta_val_ptr = delta->AccessWithNullCheck(delta_col_offset);
      byte *old_val_ptr = old->AccessWithNullCheck(old_col_offset);
      if (delta_val_ptr == nullptr) {
        EXPECT_TRUE(old_val_ptr == nullptr);
      } else {
        // check that the change has been applied
        EXPECT_TRUE(!memcmp(delta_val_ptr, old_val_ptr, layout.AttrSize(col)));
      }
    }

    // check whether other cols have been polluted
    std::unordered_set<storage::col_id_t> changed_cols(rand_col_ids.begin(), rand_col_ids.end());
    for (uint16_t i = 0; i < old->NumColumns(); ++i) {
      storage::col_id_t col_id(static_cast<uint16_t>(i + storage::NUM_RESERVED_COLUMNS));
      if (changed_cols.find(all_col_ids[i]) == changed_cols.end()) {
        byte *ptr = old->AccessWithNullCheck(i);
        if (ptr == nullptr) {
          EXPECT_TRUE(copy->IsNull(i));
        } else {
          EXPECT_TRUE(!memcmp(ptr, copy->AccessWithNullCheck(i), layout.AttrSize(col_id)));
        }
      }
    }

    delete[] copy_bufffer;
    delete[] delta_buffer;
    delete[] old_buffer;
  }
}

// Ensure that the ForceOid function for schemas works as intended
// NOLINTNEXTLINE
TEST_F(StorageUtilTests, ForceOid) {
  auto index_col = catalog::IndexSchema::Column("", type::TypeId::INTEGER, false,
                                                parser::ConstantValueExpression(type::TypeId::INTEGER));
  auto idx_col_oid = catalog::indexkeycol_oid_t(1);
  StorageTestUtil::ForceOid(&(index_col), idx_col_oid);
  EXPECT_EQ(index_col.Oid(), idx_col_oid);

  auto col = catalog::Schema::Column("iHateStorage", type::TypeId::INTEGER, false,
                                     parser::ConstantValueExpression(type::TypeId::INTEGER));
  auto col_oid = catalog::col_oid_t(2);
  StorageTestUtil::ForceOid(&(col), col_oid);
  EXPECT_EQ(col.Oid(), col_oid);
}
}  // namespace noisepage
