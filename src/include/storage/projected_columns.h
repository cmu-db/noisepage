#pragma once

#include <vector>

#include "common/container/bitmap.h"
#include "common/macros.h"
#include "common/strong_typedef.h"
#include "storage/storage_util.h"

namespace noisepage::storage {
/**
 * ProjectedColumns represents partial images of a collection of tuples, where columns from different
 * tuples are laid out continuously. This can be considered a collection of ProjectedRows, but optimized
 * for continuous column access like PAX. However, a ProjectedRow is almost always externally coupled to a known
 * tuple slot, so it is more compact in layout than MaterializedColumns, which has to also store the
 * TupleSlot information for each tuple. The inner class RowView provides access to the underlying logical
 * projected rows with the same interface as a real ProjectedRow.
 * -------------------------------------------------------------------------------------
 * | size | max_tuples | num_tuples | num_cols | attr_end[4] | col_id1 | col_id2 | ... |
 * -------------------------------------------------------------------------------------
 * | val1_offset | val2_offset | ... | TupleSlot_1 | TupleSlot_2 |         ...         |
 * -------------------------------------------------------------------------------------
 * | null-bitmap, col_id1 | val1, col_id1 | val2, col_id1 |             ...            |
 * -------------------------------------------------------------------------------------
 * | null-bitmap, col_id1 | val1, col_id2 | val2, col_id2 |             ...            |
 * -------------------------------------------------------------------------------------
 * |                                       ...                                         |
 * -------------------------------------------------------------------------------------
 */
// PACKED for the same reason as ProjectedRow
class PACKED ProjectedColumns {
 public:
  // TODO(Tianyu): This is potentially inefficient, implemented as immutable
  // although it is nicer from a software engineering standpoint, if it ends up a problem we can change it so caller
  // can change the row this view refers to.
  /**
   * A view into a row of the ProjectedColumns that has the same interface as a ProjectedRow.
   */
  class RowView {
   public:
    /**
     * @return number of columns stored in the ProjectedColumns
     */
    uint16_t NumColumns() const { return underlying_->NumColumns(); }

    /**
     * @return pointer to the start of the array of column ids
     */
    col_id_t *ColumnIds() { return underlying_->ColumnIds(); }

    /**
     * @return const pointer to the start of the array of column ids
     */
    const col_id_t *ColumnIds() const { return underlying_->ColumnIds(); }

    /**
     * Set the attribute in the row to be null using the internal bitmap
     * @param projection_list_index The 0-indexed element to access in this RowView
     */
    void SetNull(const uint16_t projection_list_index) {
      NOISEPAGE_ASSERT(projection_list_index < underlying_->NumColumns(), "Column offset out of bounds.");
      underlying_->ColumnNullBitmap(projection_list_index)->Set(row_offset_, false);
    }

    /**
     * Set the attribute in the row to be not null using the internal bitmap
     * @param projection_list_index The 0-indexed element to access in this RowView
     */
    void SetNotNull(const uint16_t projection_list_index) {
      NOISEPAGE_ASSERT(projection_list_index < underlying_->NumColumns(), "Column offset out of bounds.");
      underlying_->ColumnNullBitmap(projection_list_index)->Set(row_offset_, true);
    }

    /**
     * Check if the attribute in the RowView is null
     * @param projection_list_index The 0-indexed element to access in this RowView
     * @return true if null, false otherwise
     */
    bool IsNull(const uint16_t projection_list_index) const {
      NOISEPAGE_ASSERT(projection_list_index < underlying_->NumColumns(), "Column offset out of bounds.");
      return !underlying_->ColumnNullBitmap(projection_list_index)->Test(row_offset_);
    }

    /**
     * Access a single attribute within the RowView with a check of the null bitmap first for nullable types
     * @param projection_list_index The 0-indexed element to access in this RowView
     * @return byte pointer to the attribute. reinterpret_cast and dereference to access the value. if attribute is
     * nullable and set to null, then return value is nullptr
     */
    byte *AccessWithNullCheck(const uint16_t projection_list_index) {
      NOISEPAGE_ASSERT(projection_list_index < underlying_->NumColumns(), "Column offset out of bounds.");
      if (IsNull(projection_list_index)) return nullptr;
      return underlying_->ColumnStart(projection_list_index) +
             underlying_->AttrSizeForColumn(projection_list_index) * row_offset_;
    }

    /**
     * Access a single attribute within the RowView with a check of the null bitmap first for nullable types
     * @param projection_list_index The 0-indexed element to access in this RowView
     * @return byte pointer to the attribute. reinterpret_cast and dereference to access the value. if attribute is
     * nullable and set to null, then return value is nullptr
     */
    const byte *AccessWithNullCheck(const uint16_t projection_list_index) const {
      NOISEPAGE_ASSERT(projection_list_index < underlying_->NumColumns(), "Column offset out of bounds.");
      if (IsNull(projection_list_index)) return nullptr;
      return underlying_->ColumnStart(projection_list_index) +
             underlying_->AttrSizeForColumn(projection_list_index) * row_offset_;
    }

    /**
     * Access a single attribute within the RowView without a check of the null bitmap first
     * @param projection_list_index The 0-indexed element to access in this RowView
     * @return byte pointer to the attribute. reinterpret_cast and dereference to access the value
     */
    byte *AccessForceNotNull(const uint16_t projection_list_index) {
      NOISEPAGE_ASSERT(projection_list_index < underlying_->NumColumns(), "Column offset out of bounds.");
      if (IsNull(projection_list_index)) SetNotNull(projection_list_index);
      return underlying_->ColumnStart(projection_list_index) +
             underlying_->AttrSizeForColumn(projection_list_index) * row_offset_;
    }

   private:
    friend class ProjectedColumns;
    RowView(ProjectedColumns *underlying, uint32_t row_offset) : underlying_(underlying), row_offset_(row_offset) {}
    ProjectedColumns *const underlying_;
    const uint32_t row_offset_;
  };

  MEM_REINTERPRETATION_ONLY(ProjectedColumns)

  /**
   * @return size of this ProjectedColumns in memory, in bytes
   */
  uint32_t Size() const { return size_; }

  /**
   * @return the maximum number of tuples this ProjectedColumns can hold.
   */
  uint32_t MaxTuples() const { return max_tuples_; }

  /**
   * @return the actual number of tuples this ProjectedColumns holds. These tuples are guaranteed to be laid out in
   * offsets 0 to NumTuples() - 1
   */
  uint32_t NumTuples() { return num_tuples_; }

  /**
   * Set the number of tuples in the ProjectedColumns to be the given value
   * @param val the value to set to
   */
  void SetNumTuples(uint32_t val) { num_tuples_ = val; }

  /**
   * @return number of columns stored in the ProjectedColumns
   */
  uint16_t NumColumns() const { return num_cols_; }

  /**
   * @warning don't use these above the storage layer, they have no meaning
   * @return pointer to the start of the array of column ids
   */
  col_id_t *ColumnIds() { return reinterpret_cast<col_id_t *>(varlen_contents_); }

  /**
   * @warning don't use these above the storage layer, they have no meaning
   * @return pointer to the start of the array of column ids
   */
  const col_id_t *ColumnIds() const { return reinterpret_cast<const col_id_t *>(varlen_contents_); }

  /**
   * @return Head of the array that holds the tuple slots of the tuples currently materialized in the ProjectedColumns
   */
  storage::TupleSlot *TupleSlots() {
    return StorageUtil::AlignedPtr<storage::TupleSlot>(AttrValueOffsets() + num_cols_);
  }

  /**
   * @param projection_list_index index of the desired column in the projection list
   * @return pointer to the column presence bitmap for the given projection list column
   */
  common::RawBitmap *ColumnNullBitmap(uint16_t projection_list_index) {
    byte *column_start = reinterpret_cast<byte *>(this) + AttrValueOffsets()[projection_list_index];
    return reinterpret_cast<common::RawBitmap *>(column_start);
  }

  // TODO(Tianyu): If we make RowView mutable, then remove this function and make the constructor of RowView public.
  /**
   *
   * @param row_offset the row offset within the ProjectedColumns to look at
   * @return a view into the desired row within the ProjectedColumns
   */
  RowView InterpretAsRow(uint32_t row_offset) { return {this, row_offset}; }

  /**
   * @param projection_list_index index of the desired column in the projection list
   * @return pointer to the column value array for the given projection list column
   */
  byte *ColumnStart(uint16_t projection_list_index) {
    // TODO(Tianyu): Just pad up to 8 bytes because we do not want to store block layout?
    // We should probably be consistent with what we do in blocks, which probably means modifying blocks
    // since I don't think replicating the block layout here sounds right.
    return StorageUtil::AlignedPtr(sizeof(uint64_t), reinterpret_cast<byte *>(ColumnNullBitmap(projection_list_index)) +
                                                         common::RawBitmap::SizeInBytes(max_tuples_));
  }

  /**
   * Returns the attribute size for the corresponding column
   * @param projection_col_index the column ID within the projection we want the size for
   * @return the size (in bytes) of the attributes in this column
   */
  uint32_t AttrSizeForColumn(uint16_t projection_col_index);

 private:
  friend class ProjectedColumnsInitializer;
  uint32_t size_;
  uint32_t max_tuples_;
  uint32_t num_tuples_;
  uint16_t num_cols_;
  uint16_t attr_ends_[NUM_ATTR_BOUNDARIES];
  byte varlen_contents_[0];

  uint32_t *AttrValueOffsets() { return StorageUtil::AlignedPtr<uint32_t>(ColumnIds() + num_cols_); }
  const uint32_t *AttrValueOffsets() const { return StorageUtil::AlignedPtr<const uint32_t>(ColumnIds() + num_cols_); }
};

/**
 * A ProjectedColumnsInitializer calculates and stores information on how to initialize ProjectedColumns
 * for a specific layout. The interface is analogous to @see ProjectedRowInitializer
 */
class ProjectedColumnsInitializer {
 public:
  /**
   *  Constructs a ProjectedColumnsInitializer. Calculates the size of this ProjectedColumns, including all members,
   *  values, bitmaps, and potential padding, and the offsets to jump to for each value. This information is cached for
   *  repeated initialization. The semantics is analogous to @see ProjectedRowInitializer.
   * @param layout BlockLayout of the RawBlock to be accessed
   * @param col_ids projection list of column ids to map, should have all unique values (no repeats)
   * @param max_tuples max number of tuples the ProjectedColumns should hold
   * @warning col_ods must be a set (no repeats)
   */
  ProjectedColumnsInitializer(const BlockLayout &layout, std::vector<col_id_t> col_ids, uint32_t max_tuples);

  /**
   * Populates the ProjectedColumns's members based on projection list and BlockLayout used to construct this
   * initializer.
   * @param head pointer to the byte buffer to initialize as a ProjectionListColumns
   * @return pointer to the initialized ProjectedColumns
   */
  ProjectedColumns *Initialize(void *head) const;

  /**
   * @return size of the ProjectedColumns in memory, in bytes, that this initializer constructs.
   */
  uint32_t ProjectedColumnsSize() const { return size_; }

  /**
   * @return the maximum number of tuples this ProjectedColumns can hold.
   */
  uint32_t MaxTuples() const { return max_tuples_; }

  /**
   * @return number of columns in the projection list
   */
  uint16_t NumColumns() const { return static_cast<uint16_t>(col_ids_.size()); }

  /**
   * @return column ids at the given offset in the projection list
   */
  col_id_t ColId(uint16_t i) const { return col_ids_.at(i); }

 private:
  uint32_t size_ = 0;
  uint32_t max_tuples_;
  uint16_t attr_ends_[NUM_ATTR_BOUNDARIES];
  std::vector<col_id_t> col_ids_;
  std::vector<uint32_t> offsets_;
};
}  // namespace noisepage::storage
