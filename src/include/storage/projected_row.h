#pragma once
#include <vector>
#include "common/container/bitmap.h"
#include "common/macros.h"
#include "common/strong_typedef.h"
#include "storage/storage_util.h"

namespace terrier::storage {
// TODO(Tianyu): To be consistent with other places, maybe move val_offset fields in front of col_ids
/**
 * A projected row is a partial row image of a tuple. It also encodes
 * a projection list that allows for reordering of the columns. Its in-memory
 * layout:
 * -------------------------------------------------------------------------------
 * | size | num_cols | col_id1 | col_id2 | ... | val1_offset | val2_offset | ... |
 * -------------------------------------------------------------------------------
 * | null-bitmap (pad up to byte) | val1 | val2 | ...                            |
 * -------------------------------------------------------------------------------
 * Warning, 0 means null in the null-bitmap
 *
 * The projection list is encoded as position of col_id -> col_id. For example:
 *
 * --------------------------------------------------------
 * | 36 | 3 | 1 | 0 | 2 | 0 | 4 | 8 | 0xC0 | 721 | 15 | x |
 * --------------------------------------------------------
 * Would be the row: { 0 -> 15, 1 -> 721, 2 -> nul}
 */
// The PACKED directive here is not necessary, but C++ does some weird thing where it will pad sizeof(ProjectedRow)
// to 8 but the varlen content still points at 6. This should not break any code, but the padding now will be
// immediately before the values instead of after num_cols, which is weird.
//
// This should not have any impact because we disallow direct construction of a ProjectedRow and
// we will have control over the exact padding and layout by allocating byte arrays on the heap
// and then initializing using the static factory provided.
class PACKED ProjectedRow {
 public:
  MEM_REINTERPRETATION_ONLY(ProjectedRow)

  /**
   * Populates the ProjectedRow's members based on an existing ProjectedRow. The new ProjectedRow has the
   * same layout as the given one.
   *
   * @param head pointer to the byte buffer to initialize as a ProjectedRow
   * @param other ProjectedRow to use as template for setup
   * @return pointer to the initialized ProjectedRow
   */
  static ProjectedRow *CopyProjectedRowLayout(void *head, const ProjectedRow &other);

  /**
   * @return the size of this ProjectedRow in memory, in bytes
   */
  uint32_t Size() const { return size_; }

  /**
   * @return number of columns stored in the ProjectedRow
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
   * Access a single attribute within the ProjectedRow with a check of the null bitmap first for nullable types
   * @param offset The 0-indexed element to access in this ProjectedRow
   * @return byte pointer to the attribute. reinterpret_cast and dereference to access the value. if attribute is
   * nullable and set to null, then return value is nullptr
   */
  byte *AccessWithNullCheck(const uint16_t offset) {
    TERRIER_ASSERT(offset < num_cols_, "Column offset out of bounds.");
    if (!Bitmap().Test(offset)) return nullptr;
    return reinterpret_cast<byte *>(this) + AttrValueOffsets()[offset];
  }

  /**
   * Access a single attribute within the ProjectedRow with a check of the null bitmap first for nullable types
   * @param offset The 0-indexed element to access in this ProjectedRow
   * @return byte pointer to the attribute. reinterpret_cast and dereference to access the value. if attribute is
   * nullable and set to null, then return value is nullptr
   */
  const byte *AccessWithNullCheck(const uint16_t offset) const {
    TERRIER_ASSERT(offset < num_cols_, "Column offset out of bounds.");
    if (!Bitmap().Test(offset)) return nullptr;
    return reinterpret_cast<const byte *>(this) + AttrValueOffsets()[offset];
  }

  /**
   * Access a single attribute within the ProjectedRow without a check of the null bitmap first
   * @param offset The 0-indexed element to access in this ProjectedRow
   * @return byte pointer to the attribute. reinterpret_cast and dereference to access the value
   */
  byte *AccessForceNotNull(const uint16_t offset) {
    TERRIER_ASSERT(offset < num_cols_, "Column offset out of bounds.");
    if (!Bitmap().Test(offset)) Bitmap().Flip(offset);
    return reinterpret_cast<byte *>(this) + AttrValueOffsets()[offset];
  }

  /**
   * Set the attribute in the ProjectedRow to be null using the internal bitmap
   * @param offset The 0-indexed element to access in this ProjectedRow
   */
  void SetNull(const uint16_t offset) {
    TERRIER_ASSERT(offset < num_cols_, "Column offset out of bounds.");
    Bitmap().Set(offset, false);
  }

  /**
   * Set the attribute in the ProjectedRow to be not null using the internal bitmap
   * @param offset The 0-indexed element to access in this ProjectedRow
   */
  void SetNotNull(const uint16_t offset) {
    TERRIER_ASSERT(offset < num_cols_, "Column offset out of bounds.");
    Bitmap().Set(offset, true);
  }

  /**
   * Check if the attribute in the ProjectedRow is null
   * @param offset The 0-indexed element to access in this ProjectedRow
   * @return true if null, false otherwise
   */
  bool IsNull(const uint16_t offset) const {
    TERRIER_ASSERT(offset < num_cols_, "Column offset out of bounds.");
    return !Bitmap().Test(offset);
  }

 private:
  friend class ProjectedRowInitializer;
  friend class LogManager;
  uint32_t size_;
  uint16_t num_cols_;
  byte varlen_contents_[0];

  uint32_t *AttrValueOffsets() { return StorageUtil::AlignedPtr<uint32_t>(ColumnIds() + num_cols_); }

  const uint32_t *AttrValueOffsets() const { return StorageUtil::AlignedPtr<const uint32_t>(ColumnIds() + num_cols_); }

  common::RawBitmap &Bitmap() { return *reinterpret_cast<common::RawBitmap *>(AttrValueOffsets() + num_cols_); }

  const common::RawBitmap &Bitmap() const {
    return *reinterpret_cast<const common::RawBitmap *>(AttrValueOffsets() + num_cols_);
  }
};

/**
 * A ProjectedRowInitializer calculates and stores information on how to initialize ProjectedRows
 * for a specific layout.
 *
 * More specifically, ProjectedRowInitializer will calculate an optimal layout for the given col_ids and layout,
 * treating the vector as an unordered set of ids to include, and stores the information locally so it can be copied
 * into new ProjectedRows. It works almost like compilation, in that the initialization process is slightly more
 * expensive on the first invocation but cheaper on sequential ones. The correct way to use this is to get an
 * initializer and use it multiple times. Avoid throwing these away every time you are done.
 */
class ProjectedRowInitializer {
 public:
  /**
   * Populates the ProjectedRow's members based on projection list and BlockLayout used to construct this initializer
   * @param head pointer to the byte buffer to initialize as a ProjectedRow
   * @return pointer to the initialized ProjectedRow
   */
  ProjectedRow *InitializeRow(void *head) const;

  /**
   * @return size of the ProjectedRow in memory, in bytes, that this initializer constructs.
   */
  uint32_t ProjectedRowSize() const { return size_; }

  /**
   * @return number of columns in the projection list
   */
  uint16_t NumColumns() const { return static_cast<uint16_t>(col_ids_.size()); }

  /**
   * @return column ids at the given offset in the projection list
   */
  col_id_t ColId(uint16_t i) const { return col_ids_.at(i); }

  /**
   * Constructs a ProjectedRowInitializer. Calculates the size of this ProjectedRow, including all members, values,
   * bitmap, and potential padding, and the offsets to jump to for each value. This information is cached for repeated
   * initialization.
   *
   * @warning The ProjectedRowInitializer WILL reorder the given col_ids in its representation for better memory
   * utilization and performance. Make no assumption about the ordering of these elements and always consult either
   * the initializer or the populated ProjectedRow for the true ordering
   * @warning col_ids must be a set (no repeats)
   *
   * @param layout BlockLayout of the RawBlock to be accessed
   * @param col_ids projection list of column ids to map, should have all unique values (no repeats)
   */
  static ProjectedRowInitializer Create(const BlockLayout &layout, std::vector<col_id_t> col_ids);

  /**
   * Constructs a ProjectedRowInitializer. Calculates the size of this ProjectedRow, including all members, values,
   * bitmap, and potential padding, and the offsets to jump to for each value. This information is cached for repeated
   * initialization.
   *
   * @tparam AttrType datatype of attribute sizes
   * @param real_attr_sizes unsorted REAL attribute sizes, e.g. they shouldn't use MSB to indicate varlen.
   * @param pr_offsets pr_offsets[i] = projection list offset of attr_sizes[i] after it gets sorted
   */
  template <typename AttrType>
  static ProjectedRowInitializer Create(std::vector<AttrType> real_attr_sizes, const std::vector<uint16_t> &pr_offsets);

 private:
  /**
   * Constructs a ProjectedRowInitializer. Calculates the size of this ProjectedRow, including all members, values,
   * bitmap, and potential padding, and the offsets to jump to for each value. This information is cached for repeated
   * initialization.
   *
   * @tparam AttrType datatype of attribute sizes
   * @param attr_sizes sorted attribute sizes
   * @param col_ids column ids
   */
  template <typename AttrType>
  ProjectedRowInitializer(const std::vector<AttrType> &attr_sizes, std::vector<col_id_t> col_ids);

  uint32_t size_ = 0;
  std::vector<col_id_t> col_ids_;
  std::vector<uint32_t> offsets_;
};
}  // namespace terrier::storage
