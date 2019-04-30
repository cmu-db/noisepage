#pragma once
#include <cinttypes>
#include <cstdio>
#include <cstring>
#include <random>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include "catalog/schema.h"
#include "common/strong_typedef.h"
#include "gtest/gtest.h"
#include "storage/storage_defs.h"
#include "storage/storage_util.h"
#include "storage/tuple_access_strategy.h"
#include "storage/undo_record.h"
#include "type/type_id.h"
#include "util/multithread_test_util.h"
#include "util/random_test_util.h"

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
  static storage::BlockLayout RandomLayoutNoVarlen(const uint16_t max_cols, Random *const generator) {
    return RandomLayout(max_cols, generator, false);
  }

  template <typename Random>
  static storage::BlockLayout RandomLayoutWithVarlens(const uint16_t max_cols, Random *const generator) {
    return RandomLayout(max_cols, generator, true);
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
    std::bernoulli_distribution coin(1 - null_bias);
    // TODO(Tianyu): I don't think this matters as a tunable thing?
    // Make sure we have a mix of inlined and non-inlined values
    std::uniform_int_distribution<uint32_t> varlen_size(1, 2 * storage::VarlenEntry::InlineThreshold());
    // For every column in the project list, populate its attribute with random bytes or set to null based on coin flip
    for (uint16_t projection_list_idx = 0; projection_list_idx < row->NumColumns(); projection_list_idx++) {
      storage::col_id_t col = row->ColumnIds()[projection_list_idx];

      if (coin(*generator)) {
        if (layout.IsVarlen(col)) {
          uint32_t size = varlen_size(*generator);
          if (size > storage::VarlenEntry::InlineThreshold()) {
            byte *varlen = common::AllocationUtil::AllocateAligned(size);
            FillWithRandomBytes(size, varlen, generator);
            // varlen entries always start off not inlined
            *reinterpret_cast<storage::VarlenEntry *>(row->AccessForceNotNull(projection_list_idx)) =
                storage::VarlenEntry::Create(varlen, size, true);
          } else {
            byte buf[storage::VarlenEntry::InlineThreshold()];
            FillWithRandomBytes(size, buf, generator);
            *reinterpret_cast<storage::VarlenEntry *>(row->AccessForceNotNull(projection_list_idx)) =
                storage::VarlenEntry::CreateInline(buf, size);
          }
        } else {
          FillWithRandomBytes(layout.AttrSize(col), row->AccessForceNotNull(projection_list_idx), generator);
        }
      } else {
        row->SetNull(projection_list_idx);
      }
    }
  }

  static std::vector<storage::col_id_t> ProjectionListAllColumns(const storage::BlockLayout &layout) {
    std::vector<storage::col_id_t> col_ids(layout.NumColumns() - NUM_RESERVED_COLUMNS);
    // Add all of the column ids from the layout to the projection list
    // 0 is version vector so we skip it
    for (uint16_t col = NUM_RESERVED_COLUMNS; col < layout.NumColumns(); col++) {
      col_ids[col - NUM_RESERVED_COLUMNS] = storage::col_id_t(col);
    }
    return col_ids;
  }

  template <typename Random>
  static std::vector<storage::col_id_t> ProjectionListRandomColumns(const storage::BlockLayout &layout,
                                                                    Random *const generator) {
    // randomly select a number of columns for this delta to contain. Must be at least 1, but shouldn't be num_cols
    // since we exclude the version vector column
    uint16_t num_cols = std::uniform_int_distribution<uint16_t>(
        1, static_cast<uint16_t>(layout.NumColumns() - NUM_RESERVED_COLUMNS))(*generator);

    std::vector<storage::col_id_t> col_ids;
    // Add all of the column ids from the layout to the projection list
    // 0 is version vector so we skip it
    for (uint16_t col = NUM_RESERVED_COLUMNS; col < layout.NumColumns(); col++) col_ids.emplace_back(col);

    // permute the column ids for our random delta
    std::shuffle(col_ids.begin(), col_ids.end(), *generator);

    // truncate the projection list
    col_ids.resize(num_cols);

    return col_ids;
  }

  template <class Random>
  static std::unordered_map<storage::TupleSlot, storage::ProjectedRow *> PopulateBlockRandomly(
      const storage::BlockLayout &layout, storage::RawBlock *block, double empty_ratio, Random *const generator) {
    std::unordered_map<storage::TupleSlot, storage::ProjectedRow *> result;
    std::bernoulli_distribution coin(empty_ratio);
    // TODO(Tianyu): Do we ever want to tune this for tests?
    const double null_ratio = 0.1;
    storage::TupleAccessStrategy accessor(layout);  // Have to construct one since we don't have access to data table
    auto initializer = storage::ProjectedRowInitializer::CreateProjectedRowInitializer(
        layout, StorageTestUtil::ProjectionListAllColumns(layout));
    for (uint32_t i = 0; i < layout.NumSlots(); i++) {
      storage::TupleSlot slot;
      bool ret UNUSED_ATTRIBUTE = accessor.Allocate(block, &slot);
      TERRIER_ASSERT(ret && slot == storage::TupleSlot(block, i),
                     "slot allocation should happen sequentially and succeed");
      if (coin(*generator)) {
        // slot will be marked empty
        accessor.Deallocate(slot);
        continue;
      }
      auto *redo_buffer = common::AllocationUtil::AllocateAligned(initializer.ProjectedRowSize());
      storage::ProjectedRow *redo = initializer.InitializeRow(redo_buffer);
      StorageTestUtil::PopulateRandomRow(redo, layout, null_ratio, generator);
      result[slot] = redo;
      // Copy without transactions to simulate a version-free block
      accessor.SetNotNull(slot, VERSION_POINTER_COLUMN_ID);
      for (uint16_t j = 0; j < redo->NumColumns(); j++)
        storage::StorageUtil::CopyAttrFromProjection(accessor, slot, *redo, j);
    }
    TERRIER_ASSERT(block->insert_head_ == layout.NumSlots(), "The block should be considered full at this point");
    return result;
  }

  template <class Random>
  static storage::ProjectedRowInitializer RandomInitializer(const storage::BlockLayout &layout, Random *generator) {
    return {layout, ProjectionListRandomColumns(layout, generator)};
  }

  // Returns true iff the underlying varlen is bit-wise identical. Compressions schemes and other metadata are ignored.
  static bool VarlenEntryEqualDeep(const storage::VarlenEntry &one, const storage::VarlenEntry &other) {
    if (one.Size() != other.Size()) return false;
    return memcmp(one.Content(), other.Content(), one.Size()) == 0;
  }

  template <class RowType1, class RowType2>
  static bool ProjectionListEqualDeep(const storage::BlockLayout &layout, const RowType1 *const one,
                                      const RowType2 *const other) {
    if (one->NumColumns() != other->NumColumns()) return false;
    for (uint16_t projection_list_index = 0; projection_list_index < one->NumColumns(); projection_list_index++) {
      // Check that the two point at the same column
      storage::col_id_t one_id = one->ColumnIds()[projection_list_index];
      storage::col_id_t other_id = other->ColumnIds()[projection_list_index];
      if (one_id != other_id) return false;

      // Check that the two have the same content bit-wise
      uint8_t attr_size = layout.AttrSize(one_id);
      const byte *one_content = one->AccessWithNullCheck(projection_list_index);
      const byte *other_content = other->AccessWithNullCheck(projection_list_index);
      // Either both are null or neither is null.

      if (one_content == nullptr || other_content == nullptr) {
        if (one_content == other_content) continue;
        return false;
      }

      if (layout.IsVarlen(one_id)) {
        // Need to follow pointers and throw away metadata and padding for equality comparison
        auto &one_entry = *reinterpret_cast<const storage::VarlenEntry *>(one_content),
             &other_entry = *reinterpret_cast<const storage::VarlenEntry *>(other_content);
        if (one_entry.Size() != other_entry.Size()) return false;
        if (memcmp(one_entry.Content(), other_entry.Content(), one_entry.Size()) != 0) return false;
      } else if (memcmp(one_content, other_content, attr_size) != 0) {
        // Otherwise, they should be bit-wise identical.
        return false;
      }
    }
    return true;
  }

  template <class RowType1, class RowType2>
  static bool ProjectionListEqualShallow(const storage::BlockLayout &layout, const RowType1 *const one,
                                         const RowType2 *const other) {
    EXPECT_EQ(one->NumColumns(), other->NumColumns());
    if (one->NumColumns() != other->NumColumns()) return false;
    for (uint16_t projection_list_index = 0; projection_list_index < one->NumColumns(); projection_list_index++) {
      // Check that the two point at the same column
      storage::col_id_t one_id = one->ColumnIds()[projection_list_index];
      storage::col_id_t other_id = other->ColumnIds()[projection_list_index];
      EXPECT_EQ(one_id, other_id);
      if (one_id != other_id) return false;

      // Check that the two have the same content bit-wise
      uint8_t attr_size = layout.AttrSize(one_id);
      const byte *one_content = one->AccessWithNullCheck(projection_list_index);
      const byte *other_content = other->AccessWithNullCheck(projection_list_index);
      // Either both are null or neither is null.
      if (one_content == nullptr || other_content == nullptr) {
        EXPECT_EQ(one_content, other_content);
        if (one_content == other_content) continue;
        return false;
      }
      // Otherwise, they should be bit-wise identical.
      if (memcmp(one_content, other_content, attr_size) != 0) return false;
    }
    return true;
  }

  template <class RowType>
  static std::string PrintRow(const RowType &row, const storage::BlockLayout &layout) {
    std::ostringstream os;
    os << "num_cols: " << row.NumColumns() << std::endl;
    for (uint16_t i = 0; i < row.NumColumns(); i++) {
      storage::col_id_t col_id = row.ColumnIds()[i];
      const byte *attr = row.AccessWithNullCheck(i);
      if (attr == nullptr) {
        os << "col_id: " << !col_id << " is NULL" << std::endl;
        continue;
      }

      if (layout.IsVarlen(col_id)) {
        auto *entry = reinterpret_cast<const storage::VarlenEntry *>(attr);
        os << "col_id: " << !col_id;
        os << " is varlen, ptr " << entry->Content();
        os << ", size " << entry->Size();
        os << ", reclaimable " << entry->NeedReclaim();
        os << ", content ";
        for (uint8_t pos = 0; pos < entry->Size(); pos++) {
          os << std::setfill('0') << std::setw(2) << std::hex << static_cast<uint8_t>(entry->Content()[pos]);
        }
        os << std::endl;
      } else {
        os << "col_id: " << !col_id;
        os << " is ";
        for (uint8_t pos = 0; pos < layout.AttrSize(col_id); pos++) {
          os << std::setfill('0') << std::setw(2) << std::hex << static_cast<uint8_t>(attr[pos]);
        }
        os << std::endl;
      }
    }
    return os.str();
  }

  // Write the given tuple (projected row) into a block using the given access strategy,
  // at the specified offset
  static void InsertTuple(const storage::ProjectedRow &tuple, const storage::TupleAccessStrategy &tested,
                          const storage::BlockLayout &layout, const storage::TupleSlot slot) {
    // Skip the version vector for tuples
    for (uint16_t projection_list_index = 0; projection_list_index < tuple.NumColumns(); projection_list_index++) {
      storage::col_id_t col_id(static_cast<uint16_t>(projection_list_index + NUM_RESERVED_COLUMNS));
      const byte *val_ptr = tuple.AccessWithNullCheck(projection_list_index);
      if (val_ptr == nullptr)
        tested.SetNull(slot, storage::col_id_t(projection_list_index));
      else
        std::memcpy(tested.AccessForceNotNull(slot, col_id), val_ptr, layout.AttrSize(col_id));
    }
  }

  // Check that the written tuple is the same as the expected one. Does not follow varlen pointers to check that the
  // underlying values are the same
  static void CheckTupleEqualShallow(const storage::ProjectedRow &expected, const storage::TupleAccessStrategy &tested,
                                     const storage::BlockLayout &layout, const storage::TupleSlot slot) {
    for (uint16_t col = NUM_RESERVED_COLUMNS; col < layout.NumColumns(); col++) {
      storage::col_id_t col_id(col);
      const byte *val_ptr = expected.AccessWithNullCheck(static_cast<uint16_t>(col - NUM_RESERVED_COLUMNS));
      byte *col_slot = tested.AccessWithNullCheck(slot, col_id);
      if (val_ptr == nullptr) {
        EXPECT_TRUE(col_slot == nullptr);
      } else {
        EXPECT_TRUE(!memcmp(val_ptr, col_slot, layout.AttrSize(col_id)));
      }
    }
  }

 private:
  template <typename Random>
  static storage::BlockLayout RandomLayout(const uint16_t max_cols, Random *const generator, bool allow_varlen) {
    TERRIER_ASSERT(max_cols > NUM_RESERVED_COLUMNS, "There should be at least 2 cols (reserved for version).");
    // We probably won't allow tables with fewer than 2 columns
    const uint16_t num_attrs = std::uniform_int_distribution<uint16_t>(NUM_RESERVED_COLUMNS + 1, max_cols)(*generator);
    std::vector<uint8_t> possible_attr_sizes{1, 2, 4, 8}, attr_sizes(num_attrs);
    if (allow_varlen) possible_attr_sizes.push_back(VARLEN_COLUMN);

    for (uint16_t i = 0; i < NUM_RESERVED_COLUMNS; i++) {
      attr_sizes[i] = 8;
    }

    for (uint16_t i = NUM_RESERVED_COLUMNS; i < num_attrs; i++)
      attr_sizes[i] = *RandomTestUtil::UniformRandomElement(&possible_attr_sizes, generator);
    return storage::BlockLayout(attr_sizes);
  }
};
}  // namespace terrier
