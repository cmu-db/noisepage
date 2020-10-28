#include "storage/tuple_access_strategy.h"

#include <algorithm>
#include <cstring>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/strong_typedef.h"
#include "storage/storage_util.h"
#include "storage/undo_record.h"
#include "test_util/multithread_test_util.h"
#include "test_util/storage_test_util.h"
#include "test_util/test_harness.h"

namespace noisepage {

class TupleAccessStrategyTestObject {
 public:
  ~TupleAccessStrategyTestObject() {
    for (auto entry : loose_pointers_) {
      delete[] entry;
    }
  }

  // Using the given random generator, attempts to allocate a slot and write a
  // random tuple into it. The slot and the tuple are logged in the given map.
  // Checks are performed to make sure the insertion is sensible.
  template <typename Random>
  std::pair<const storage::TupleSlot, storage::ProjectedRow *> &TryInsertFakeTuple(
      const storage::BlockLayout &layout, const storage::TupleAccessStrategy &tested, storage::RawBlock *block,
      std::unordered_map<storage::TupleSlot, storage::ProjectedRow *> *tuples, Random *generator) {
    storage::TupleSlot slot;
    // There should always be enough slots.
    EXPECT_TRUE(tested.Allocate(block, &slot));
    EXPECT_TRUE(tested.Allocated(slot));

    // Generate a random ProjectedRow to insert
    storage::ProjectedRowInitializer initializer =
        storage::ProjectedRowInitializer::Create(layout, StorageTestUtil::ProjectionListAllColumns(layout));
    auto *buffer = common::AllocationUtil::AllocateAligned(initializer.ProjectedRowSize());
    storage::ProjectedRow *row = initializer.InitializeRow(buffer);
    std::default_random_engine real_generator;
    std::uniform_real_distribution<double> distribution{0.0, 1.0};
    StorageTestUtil::PopulateRandomRow(row, layout, distribution(real_generator), generator);
    loose_pointers_.push_back(buffer);

    auto result = tuples->emplace(std::make_pair(slot, row));

    // The tuple slot is not something that is already in use.
    EXPECT_TRUE(result.second);
    for (uint16_t projection_list_id = 0; projection_list_id < initializer.NumColumns(); projection_list_id++)
      storage::StorageUtil::CopyAttrFromProjection(tested, slot, *(result.first->second), projection_list_id);
    return *(result.first);
  }

 private:
  std::vector<byte *> loose_pointers_;
};

struct TupleAccessStrategyTests : public TerrierTest {
  storage::RawBlock *raw_block_ = nullptr;
  storage::BlockStore block_store_{1, 1};

 protected:
  void SetUp() override { raw_block_ = block_store_.Get(); }

  void TearDown() override { block_store_.Release(raw_block_); }
};

// Tests that we can set things to null and the access strategy returns nullptr for null fields.
// NOLINTNEXTLINE
TEST_F(TupleAccessStrategyTests, Nulls) {
  std::default_random_engine generator;
  const uint32_t repeat = 10;
  for (uint32_t i = 0; i < repeat; i++) {
    storage::BlockLayout layout = StorageTestUtil::RandomLayoutNoVarlen(common::Constants::MAX_COL, &generator);
    storage::TupleAccessStrategy tested(layout);
    std::memset(reinterpret_cast<void *>(raw_block_), 0, sizeof(storage::RawBlock));
    tested.InitializeRawBlock(nullptr, raw_block_, storage::layout_version_t(0));

    storage::TupleSlot slot;
    EXPECT_TRUE(tested.Allocate(raw_block_, &slot));
    std::vector<bool> nulls(layout.NumColumns());
    std::bernoulli_distribution coin(0.5);
    // Randomly set some columns to be not null
    for (uint16_t col = 0; col < layout.NumColumns(); col++) {
      if (coin(generator)) {
        nulls[col] = true;
      } else {
        nulls[col] = false;
        tested.AccessForceNotNull(slot, storage::col_id_t(col));
      }
    }

    for (uint16_t col = 0; col < layout.NumColumns(); col++) {
      // Either the field is null and the access returns nullptr,
      // or the field is not null and the access ptr is not null
      EXPECT_TRUE((tested.AccessWithNullCheck(slot, storage::col_id_t(col)) != nullptr) ^ nulls[col]);
    }

    // Flip non-null columns to null should result in returning of nullptr.
    for (uint16_t col = 1; col < layout.NumColumns(); col++) {
      if (!nulls[col]) tested.SetNull(slot, storage::col_id_t(col));
      EXPECT_TRUE(tested.AccessWithNullCheck(slot, storage::col_id_t(col)) == nullptr);
    }
  }
}

// Tests that we can allocate a tuple slot, write things into the slot and get them out.
// NOLINTNEXTLINE
TEST_F(TupleAccessStrategyTests, SimpleInsert) {
  const uint32_t repeat = 50;
  const uint32_t max_cols = 100;
  std::default_random_engine generator;
  for (uint32_t i = 0; i < repeat; i++) {
    TupleAccessStrategyTestObject test_obj;

    storage::BlockLayout layout = StorageTestUtil::RandomLayoutNoVarlen(max_cols, &generator);
    storage::TupleAccessStrategy tested(layout);
    std::memset(reinterpret_cast<void *>(raw_block_), 0, sizeof(storage::RawBlock));
    tested.InitializeRawBlock(nullptr, raw_block_, storage::layout_version_t(0));

    uint32_t num_inserts = std::uniform_int_distribution<uint32_t>(1, layout.NumSlots())(generator);

    std::unordered_map<storage::TupleSlot, storage::ProjectedRow *> tuples;
    for (uint32_t j = 0; j < num_inserts; j++) {
      test_obj.TryInsertFakeTuple(layout, tested, raw_block_, &tuples, &generator);
    }
    // Check that all inserted tuples are equal to their expected values
    for (auto &entry : tuples) {
      StorageTestUtil::CheckTupleEqualShallow(*(entry.second), tested, layout, entry.first);
    }
  }
}

// This test generates randomized block layouts, and checks its layout to ensure
// that the column bitmaps, and the columns don't overlap, and don't
// go out of page boundary. (In other words, memory safe.)
// NOLINTNEXTLINE
TEST_F(TupleAccessStrategyTests, MemorySafety) {
  const uint32_t repeat = 100;
  std::default_random_engine generator;
  for (uint32_t i = 0; i < repeat; i++) {
    storage::BlockLayout layout;
    if (i == 0) {
      // Make sure we test the largest layout so that there is still at least one slot in a block.
      std::vector<uint16_t> sizes;
      for (uint32_t j = 0; j < common::Constants::MAX_COL; j++)
        sizes.push_back(static_cast<uint16_t>(j == 0 ? 8 : storage::VARLEN_COLUMN));
      layout = storage::BlockLayout{sizes};
    } else {
      layout = StorageTestUtil::RandomLayoutWithVarlens(common::Constants::MAX_COL, &generator);
    }
    storage::TupleAccessStrategy tested(layout);
    // here we don't need to 0-initialize the block because we only
    // test layout, not the content.
    tested.InitializeRawBlock(nullptr, raw_block_, storage::layout_version_t(0));

    // Skip header
    void *lower_bound = tested.ColumnNullBitmap(raw_block_, storage::VERSION_POINTER_COLUMN_ID);
    void *upper_bound = raw_block_ + sizeof(storage::RawBlock);
    for (uint16_t offset = 0; offset < layout.NumColumns(); offset++) {
      storage::col_id_t col_id(offset);
      // This test should be robust against any future paddings, since
      // we are checking for non-overlapping ranges and not hard-coded
      // boundaries.
      StorageTestUtil::CheckInBounds(tested.ColumnNullBitmap(raw_block_, col_id), lower_bound, upper_bound);
      lower_bound = StorageTestUtil::IncrementByBytes(tested.ColumnNullBitmap(raw_block_, col_id),
                                                      common::RawBitmap::SizeInBytes(layout.NumSlots()));

      StorageTestUtil::CheckInBounds(tested.ColumnStart(raw_block_, col_id), lower_bound, upper_bound);

      lower_bound = StorageTestUtil::IncrementByBytes(tested.ColumnStart(raw_block_, col_id),
                                                      layout.NumSlots() * layout.AttrSize(col_id));
    }
    // check that the last column does not go out of the block
    uint32_t last_column_size =
        layout.NumSlots() * layout.AttrSize(storage::col_id_t(static_cast<uint16_t>(layout.NumColumns() - 1)));
    StorageTestUtil::CheckInBounds(StorageTestUtil::IncrementByBytes(lower_bound, last_column_size), lower_bound,
                                   upper_bound);
  }
}

// This test generates randomized block layouts, and checks its layout to ensure
// that each columns null bitmap is aligned to 8 bytes, and that each column start is aligned to its attribute size.
// These properties are necessary to ensure high performance by accessing aligned fields.
// NOLINTNEXTLINE
TEST_F(TupleAccessStrategyTests, Alignment) {
  const uint32_t repeat = 100;
  std::default_random_engine generator;
  StorageTestUtil::CheckAlignment(raw_block_, common::Constants::BLOCK_SIZE);
  for (uint32_t i = 0; i < repeat; i++) {
    storage::BlockLayout layout = StorageTestUtil::RandomLayoutWithVarlens(common::Constants::MAX_COL, &generator);
    storage::TupleAccessStrategy tested(layout);
    // here we don't need to 0-initialize the block because we only
    // test layout, not the content.
    tested.InitializeRawBlock(nullptr, raw_block_, storage::layout_version_t(0));

    for (uint16_t j = 0; j < layout.NumColumns(); j++) {
      storage::col_id_t col_id(j);
      auto alignment = layout.AttrSize(col_id) > 8 ? 8 : layout.AttrSize(col_id);  // no need to align above 8 bytes
      StorageTestUtil::CheckAlignment(tested.ColumnStart(raw_block_, col_id), alignment);
      StorageTestUtil::CheckAlignment(tested.ColumnNullBitmap(raw_block_, col_id), 8);
    }
  }
}
}  // namespace noisepage
