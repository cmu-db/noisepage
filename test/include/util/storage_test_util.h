#pragma once
#include <cinttypes>
#include <cstdio>
#include <random>
#include <unordered_map>
#include <utility>
#include <vector>
#include "common/typedefs.h"
#include "gtest/gtest.h"
#include "storage/storage_defs.h"
#include "storage/storage_util.h"
#include "util/multi_threaded_test_util.h"

namespace terrier {

struct StorageTestUtil {
  StorageTestUtil() = delete;

#define TO_INT(p) reinterpret_cast<uintptr_t>(p)
  /**
   * Check if memory address represented by val in [lower, upper)
   * @tparam A type of ptr
   * @tparam B type of ptr
   * @tparam C type of ptr
   * @param val value to check
   * @param lower lower bound
   * @param upper upper bound
   */
  template <typename A, typename B, typename C>
  static void CheckInBounds(A *val, B *lower, C *upper) {
    EXPECT_GE(TO_INT(val), TO_INT(lower));
    EXPECT_LT(TO_INT(val), TO_INT(upper));
  }

  /**
   * Check if memory address represented by val not in [lower, upper)
   * @tparam A type of ptr
   * @tparam B type of ptr
   * @tparam C type of ptr
   * @param val value to check
   * @param lower lower bound
   * @param upper upper bound
   */
  template <typename A, typename B, typename C>
  static void CheckNotInBounds(A *val, B *lower, C *upper) {
    EXPECT_TRUE(TO_INT(val) < TO_INT(lower) || TO_INT(val) >= TO_INT(upper));
  }

  template <typename A>
  static void CheckAlignment(A *val, uint32_t word_size) {
    EXPECT_EQ(0, TO_INT(val) % word_size);
  }
#undef TO_INT
  /**
   * @tparam A type of ptr
   * @param ptr ptr to start from
   * @param bytes bytes to advance
   * @return  pointer that is the specified amount of bytes ahead of the given
   */
  template <typename A>
  static A *IncrementByBytes(A *ptr, uint64_t bytes) {
    return reinterpret_cast<A *>(reinterpret_cast<byte *>(ptr) + bytes);
  }

  // Returns a random layout that is guaranteed to be valid.
  template <typename Random>
  static storage::BlockLayout RandomLayout(uint16_t max_cols, Random *generator) {
    PELOTON_ASSERT(max_cols > 1, "There should be at least 2 cols (first is version).");
    // We probably won't allow tables with fewer than 2 columns
    uint16_t num_attrs = std::uniform_int_distribution<uint16_t>(2, max_cols)(*generator);
    std::vector<uint8_t> possible_attr_sizes{1, 2, 4, 8}, attr_sizes(num_attrs);
    attr_sizes[0] = 8;
    for (uint16_t i = 1; i < num_attrs; i++)
      attr_sizes[i] = *MultiThreadedTestUtil::UniformRandomElement(&possible_attr_sizes, generator);
    return {num_attrs, attr_sizes};
  }

  // Fill the given location with the specified amount of random bytes, using the
  // given generator as a source of randomness.
  template <typename Random>
  static void FillWithRandomBytes(uint32_t num_bytes, byte *out, Random *generator) {
    std::uniform_int_distribution<uint8_t> dist(0, UINT8_MAX);
    for (uint32_t i = 0; i < num_bytes; i++) out[i] = static_cast<byte>(dist(*generator));
  }

  template <typename Random>
  static void PopulateRandomRow(storage::ProjectedRow *row, const storage::BlockLayout &layout, const double null_bias,
                                Random *generator) {
    // For every column in the project list, populate its attribute with random bytes or set to null based on coin flip
    for (uint16_t projection_list_idx = 0; projection_list_idx < row->NumColumns(); projection_list_idx++) {
      uint16_t col = row->ColumnIds()[projection_list_idx];
      std::bernoulli_distribution coin(1 - null_bias);

      if (coin(*generator)) {
        FillWithRandomBytes(layout.attr_sizes_[col], row->AccessForceNotNull(projection_list_idx), generator);
      }
    }
  }

  static std::vector<uint16_t> ProjectionListAllColumns(const storage::BlockLayout &layout) {
    std::vector<uint16_t> col_ids(layout.num_cols_ - 1u);
    // Add all of the column ids from the layout to the projection list
    // 0 is version vector so we skip it
    for (uint16_t col = 1; col < layout.num_cols_; col++) {
      col_ids[col - 1] = col;
    }
    return col_ids;
  }

  template <typename Random>
  static std::vector<uint16_t> ProjectionListRandomColumns(const storage::BlockLayout &layout, Random *generator) {
    // randomly select a number of columns for this delta to contain. Must be at least 1, but shouldn't be num_cols
    // since we exclude the version vector column
    uint16_t num_cols =
        std::uniform_int_distribution<uint16_t>(1, static_cast<uint16_t>(layout.num_cols_ - 1))(*generator);

    std::vector<uint16_t> col_ids;
    // Add all of the column ids from the layout to the projection list
    // 0 is version vector so we skip it
    for (uint16_t col = 1; col < layout.num_cols_; col++) {
      col_ids.push_back(col);
    }

    // permute the column ids for our random delta
    std::shuffle(col_ids.begin(), col_ids.end(), *generator);

    // truncate the projection list
    col_ids.resize(num_cols);

    return col_ids;
  }

  static bool ProjectionListEqual(const storage::BlockLayout &layout, const storage::ProjectedRow *one,
                                  const storage::ProjectedRow *other) {
    if (one->NumColumns() != other->NumColumns()) return false;
    for (uint16_t projection_list_index = 0; projection_list_index < one->NumColumns(); projection_list_index++) {
      if (one->ColumnIds()[projection_list_index] != other->ColumnIds()[projection_list_index]) return false;
    }

    for (uint16_t projection_list_index = 0; projection_list_index < one->NumColumns(); projection_list_index++) {
      uint8_t attr_size = layout.attr_sizes_[one->ColumnIds()[projection_list_index]];
      const byte *one_content = one->AccessWithNullCheck(projection_list_index);
      const byte *other_content = other->AccessWithNullCheck(projection_list_index);

      if (one_content == nullptr || other_content == nullptr) {
        if (one_content == other_content)
          continue;
        else
          return false;
      }

      if (storage::StorageUtil::ReadBytes(attr_size, one_content) !=
          storage::StorageUtil::ReadBytes(attr_size, other_content))
        return false;
    }

    return true;
  }

  static void PrintRow(const storage::ProjectedRow *row, const storage::BlockLayout &layout) {
    printf("num_cols: %u\n", row->NumColumns());
    for (uint16_t i = 0; i < row->NumColumns(); i++) {
      uint16_t col_id = row->ColumnIds()[i];
      const byte *attr = row->AccessWithNullCheck(i);
      if (attr) {
        printf("col_id: %u is %" PRIx64 "\n", col_id,
               storage::StorageUtil::ReadBytes(layout.attr_sizes_[col_id], attr));
      } else {
        printf("col_id: %u is NULL\n", col_id);
      }
    }
  }
};

// This does NOT return a sensible tuple in general. This is just some filler
// to write into the storage layer and is devoid of meaning outside of this class.
struct FakeRawTuple {
  template <typename Random>
  FakeRawTuple(const storage::BlockLayout &layout, Random *generator);
  ~FakeRawTuple() { delete[] contents_; }

  // Since all fields we store in pages are equal to or shorter than 8 bytes,
  // we can do equality checks on uint64_t always.
  // 0 return for non-primary key indexes should be treated as null.
  uint64_t Attribute(const storage::BlockLayout &layout, uint16_t col) const {
    return storage::StorageUtil::ReadBytes(layout.attr_sizes_[col], contents_ + attr_offsets_[col]);
  }

  const storage::BlockLayout &layout_;
  std::vector<uint32_t> attr_offsets_;
  byte *contents_;
};

struct TupleAccessStrategyTestUtil {
  TupleAccessStrategyTestUtil() = delete;
  // Fill the given location with the specified amount of random bytes, using the
  // given generator as a source of randomness.
  template <typename Random>
  static void FillWithRandomBytes(uint32_t num_bytes, byte *out, Random *generator) {
    std::uniform_int_distribution<uint8_t> dist(0, UINT8_MAX);
    for (uint32_t i = 0; i < num_bytes; i++) out[i] = static_cast<byte>(dist(*generator));
  }

  // Write the given fake tuple into a block using the given access strategy,
  // at the specified offset
  static void InsertTuple(const FakeRawTuple &tuple, const storage::TupleAccessStrategy *tested,
                          const storage::BlockLayout &layout, const storage::TupleSlot slot) {
    for (uint16_t col = 0; col < layout.num_cols_; col++) {
      uint64_t col_val = tuple.Attribute(layout, col);
      if (col_val != 0 || col == PRESENCE_COLUMN_ID)
        storage::StorageUtil::WriteBytes(layout.attr_sizes_[col], tuple.Attribute(layout, col),
                                         tested->AccessForceNotNull(slot, col));
      else
        tested->SetNull(slot, col);
      // Otherwise leave the field as null.
    }
  }

  // Check that the written tuple is the same as the expected one
  static void CheckTupleEqual(const FakeRawTuple &expected, storage::TupleAccessStrategy *tested,
                              const storage::BlockLayout &layout, const storage::TupleSlot slot) {
    for (uint16_t col = 0; col < layout.num_cols_; col++) {
      uint64_t expected_col = expected.Attribute(layout, col);
      // 0 return for non-primary key indexes should be treated as null.
      bool null = (expected_col == 0) && (col != PRESENCE_COLUMN_ID);
      byte *col_slot = tested->AccessWithNullCheck(slot, col);
      if (!null) {
        EXPECT_TRUE(col_slot != nullptr);
        EXPECT_EQ(expected.Attribute(layout, col), storage::StorageUtil::ReadBytes(layout.attr_sizes_[col], col_slot));
      } else {
        EXPECT_TRUE(col_slot == nullptr);
      }
    }
  }

  // Using the given random generator, attempts to allocate a slot and write a
  // random tuple into it. The slot and the tuple are logged in the given map.
  // Checks are performed to make sure the insertion is sensible.
  template <typename Random>
  static std::pair<const storage::TupleSlot, FakeRawTuple> &TryInsertFakeTuple(
      const storage::BlockLayout &layout, const storage::TupleAccessStrategy &tested, storage::RawBlock *block,
      std::unordered_map<storage::TupleSlot, FakeRawTuple> *tuples, Random *generator) {
    storage::TupleSlot slot;
    // There should always be enough slots.
    EXPECT_TRUE(tested.Allocate(block, &slot));
    EXPECT_TRUE(tested.ColumnNullBitmap(block, PRESENCE_COLUMN_ID)->Test(slot.GetOffset()));

    // Construct a random tuple and associate it with the tuple slot
    auto result = tuples->emplace(std::piecewise_construct, std::forward_as_tuple(slot),
                                  std::forward_as_tuple(layout, generator));
    // The tuple slot is not something that is already in use.
    EXPECT_TRUE(result.second);
    InsertTuple(result.first->second, &tested, layout, slot);
    return *(result.first);
  }
};

template <class Random>
FakeRawTuple::FakeRawTuple(const terrier::storage::BlockLayout &layout, Random *generator)
    : layout_(layout), attr_offsets_(), contents_(new byte[layout.tuple_size_]) {
  uint32_t pos = 0;
  for (uint16_t col = 0; col < layout.num_cols_; col++) {
    attr_offsets_.push_back(pos);
    pos += layout.attr_sizes_[col];
  }
  TupleAccessStrategyTestUtil::FillWithRandomBytes(layout.tuple_size_, contents_, generator);
}

}  // namespace terrier
