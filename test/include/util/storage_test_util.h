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
#include "storage/tuple_access_strategy.h"
#include "storage/undo_record.h"
#include "util/random_test_util.h"
#include "util/test_thread_pool.h"

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
  static void CheckInBounds(A *const val, B *const lower, C *const upper) {
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
  static void CheckNotInBounds(A *const val, B *const lower, C *const upper) {
    EXPECT_TRUE(TO_INT(val) < TO_INT(lower) || TO_INT(val) >= TO_INT(upper));
  }

  template <typename A>
  static void CheckAlignment(A *const val, const uint32_t word_size) {
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
  static A *IncrementByBytes(A *const ptr, const uint64_t bytes) {
    return reinterpret_cast<A *>(reinterpret_cast<byte *>(ptr) + bytes);
  }

  // Returns a random layout that is guaranteed to be valid.
  template <typename Random>
  static storage::BlockLayout RandomLayout(const uint16_t max_cols, Random *const generator) {
    TERRIER_ASSERT(max_cols > 2, "There should be at least 3 cols (first is version, second is logical delete).");
    // We probably won't allow tables with fewer than 2 columns
    const uint16_t num_attrs = std::uniform_int_distribution<uint16_t>(3, max_cols)(*generator);
    std::vector<uint8_t> possible_attr_sizes{1, 2, 4, 8}, attr_sizes(num_attrs);
    attr_sizes[0] = 8;
    attr_sizes[1] = 8;
    for (uint16_t i = 2; i < num_attrs; i++)
      attr_sizes[i] = *RandomTestUtil::UniformRandomElement(&possible_attr_sizes, generator);
    return storage::BlockLayout(attr_sizes);
  }

  // Fill the given location with the specified amount of random bytes, using the
  // given generator as a source of randomness.
  template <typename Random>
  static void FillWithRandomBytes(const uint32_t num_bytes, byte *const out, Random *const generator) {
    std::uniform_int_distribution<uint8_t> dist(0, UINT8_MAX);
    for (uint32_t i = 0; i < num_bytes; i++) out[i] = static_cast<byte>(dist(*generator));
  }

  template <typename Random>
  static void PopulateRandomRow(storage::ProjectedRow *const row, const storage::BlockLayout &layout,
                                const double null_bias, Random *const generator) {
    // For every column in the project list, populate its attribute with random bytes or set to null based on coin flip
    for (uint16_t projection_list_idx = 0; projection_list_idx < row->NumColumns(); projection_list_idx++) {
      col_id_t col = row->ColumnIds()[projection_list_idx];
      std::bernoulli_distribution coin(1 - null_bias);

      if (coin(*generator))
        FillWithRandomBytes(layout.AttrSize(col), row->AccessForceNotNull(projection_list_idx), generator);
      else
        row->SetNull(projection_list_idx);
    }
  }

  static std::vector<col_id_t> ProjectionListAllColumns(const storage::BlockLayout &layout) {
    std::vector<col_id_t> col_ids(layout.NumCols() - 2u);
    // Add all of the column ids from the layout to the projection list
    // 0 is version vector so we skip it
    for (uint16_t col = 2; col < layout.NumCols(); col++) {
      col_ids[col - 2] = col_id_t(col);
    }
    return col_ids;
  }

  template <typename Random>
  static std::vector<col_id_t> ProjectionListRandomColumns(const storage::BlockLayout &layout,
                                                           Random *const generator) {
    // randomly select a number of columns for this delta to contain. Must be at least 1, but shouldn't be num_cols
    // since we exclude the version vector column
    uint16_t num_cols =
        std::uniform_int_distribution<uint16_t>(1, static_cast<uint16_t>(layout.NumCols() - 2))(*generator);

    std::vector<col_id_t> col_ids;
    // Add all of the column ids from the layout to the projection list
    // 0 is version vector so we skip it
    for (uint16_t col = 2; col < layout.NumCols(); col++) col_ids.emplace_back(col);

    // permute the column ids for our random delta
    std::shuffle(col_ids.begin(), col_ids.end(), *generator);

    // truncate the projection list
    col_ids.resize(num_cols);

    return col_ids;
  }

  template <class Random>
  static storage::ProjectedRowInitializer RandomInitializer(const storage::BlockLayout &layout, Random *generator) {
    return {layout, ProjectionListRandomColumns(layout, generator)};
  }

  static bool ProjectionListEqual(const storage::BlockLayout &layout, const storage::ProjectedRow *const one,
                                  const storage::ProjectedRow *const other) {
    if (one->NumColumns() != other->NumColumns()) return false;
    for (uint16_t projection_list_index = 0; projection_list_index < one->NumColumns(); projection_list_index++) {
      if (one->ColumnIds()[projection_list_index] != other->ColumnIds()[projection_list_index]) return false;
    }

    for (uint16_t projection_list_index = 0; projection_list_index < one->NumColumns(); projection_list_index++) {
      uint8_t attr_size = layout.AttrSize(one->ColumnIds()[projection_list_index]);
      const byte *one_content = one->AccessWithNullCheck(projection_list_index);
      const byte *other_content = other->AccessWithNullCheck(projection_list_index);

      if (one_content == nullptr || other_content == nullptr) {
        if (one_content == other_content) continue;
        return false;
      }

      if (storage::StorageUtil::ReadBytes(attr_size, one_content) !=
          storage::StorageUtil::ReadBytes(attr_size, other_content))
        return false;
    }

    return true;
  }

  static void PrintRow(const storage::ProjectedRow &row, const storage::BlockLayout &layout) {
    printf("num_cols: %u\n", row.NumColumns());
    for (uint16_t i = 0; i < row.NumColumns(); i++) {
      col_id_t col_id = row.ColumnIds()[i];
      const byte *attr = row.AccessWithNullCheck(i);
      if (attr != nullptr) {
        printf("col_id: %u is %" PRIx64 "\n", !col_id, storage::StorageUtil::ReadBytes(layout.AttrSize(col_id), attr));
      } else {
        printf("col_id: %u is NULL\n", !col_id);
      }
    }
  }

  // Write the given tuple (projected row) into a block using the given access strategy,
  // at the specified offset
  static void InsertTuple(const storage::ProjectedRow &tuple, const storage::TupleAccessStrategy &tested,
                          const storage::BlockLayout &layout, const storage::TupleSlot slot) {
    // Skip the version vector for tuples
    for (uint16_t col = 1; col < layout.NumCols(); col++) {
      const byte *val_ptr = tuple.AccessWithNullCheck(static_cast<uint16_t>(col - 1));
      if (val_ptr == nullptr) {
        tested.SetNull(slot, col_id_t(col));
      } else {
        // Read the value
        uint64_t val = storage::StorageUtil::ReadBytes(layout.AttrSize(col_id_t(col)), val_ptr);
        storage::StorageUtil::WriteBytes(layout.AttrSize(col_id_t(col)), val,
                                         tested.AccessForceNotNull(slot, col_id_t(col)));
      }
    }
  }

  // Check that the written tuple is the same as the expected one
  static void CheckTupleEqual(const storage::ProjectedRow &expected, const storage::TupleAccessStrategy &tested,
                              const storage::BlockLayout &layout, const storage::TupleSlot slot) {
    for (uint16_t col = 1; col < layout.NumCols(); col++) {
      const byte *val_ptr = expected.AccessWithNullCheck(static_cast<uint16_t>(col - 1));
      byte *col_slot = tested.AccessWithNullCheck(slot, col_id_t(col));
      if (val_ptr != nullptr) {
        // Read the value
        uint64_t val = storage::StorageUtil::ReadBytes(layout.AttrSize(col_id_t(col)), val_ptr);
        EXPECT_TRUE(col_slot != nullptr);
        EXPECT_EQ(val, storage::StorageUtil::ReadBytes(layout.AttrSize(col_id_t(col)), col_slot));
      } else {
        EXPECT_TRUE(col_slot == nullptr);
      }
    }
  }
};
}  // namespace terrier
