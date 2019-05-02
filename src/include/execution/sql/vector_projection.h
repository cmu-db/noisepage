#pragma once

#include <memory>
#include <vector>

#include "execution/sql/schema.h"
#include "execution/util/bit_util.h"
#include "execution/util/common.h"
#include "execution/util/macros.h"

namespace tpl::sql {

class ColumnVectorIterator;

/// A VectorProjection is a container representing a logical collection of
/// tuples whose columns are stored in columnar format
class VectorProjection {
 public:
  /// Create a vector projection using the column information provided in
  /// \a col_infos. The vector projection stores vectors of size \a size.
  VectorProjection(const std::vector<const Schema::ColumnInfo *> &col_infos,
                   u32 size);

  /// This class cannot be copied or moved
  DISALLOW_COPY_AND_MOVE(VectorProjection);

  /// Get the metadata for the column at the given index in this projection
  const Schema::ColumnInfo *GetColumnInfo(u32 col_idx) const {
    return column_info_[col_idx];
  }

  /// Get the current vector input for the column at index \a col_idx
  /// \tparam T The data type to interpret the column's data as
  /// \param col_idx The index of the column
  /// \return The typed vector of column data in this vector projection
  template <typename T>
  const T *GetVectorAs(u32 col_idx) const {
    return reinterpret_cast<T *>(column_data_[col_idx]);
  }

  /// Return the NULL bit vector for the column at index \a col_idx
  /// \param col_idx The index of the column
  /// \return The NULL bit vector for the desired column
  const u32 *GetNullBitmap(u32 col_idx) const {
    return column_null_bitmaps_[col_idx];
  }

  /// Reset/reload the data for the column at the given index from the given
  /// column iterator instance
  /// \param col_iters A vector of all column iterators
  /// \param col_idx The index of the column in this projection to reset
  void ResetColumn(const std::vector<ColumnVectorIterator> &col_iters,
                   u32 col_idx);

  /// Reset the column data at index \a col_idx with \a col_data and the
  /// \param col_data The raw (potentially compressed) data for the column
  /// \param col_null_bitmap The null bitmap for the column
  /// \param col_idx The index of the column to reset
  /// \param num_tuples The number of tuples stored in the input
  void ResetFromRaw(byte col_data[], u32 col_null_bitmap[], u32 col_idx,
                    u32 num_tuples);

  /// Return the number of active tuples in this projection
  /// \return The number of active tuples
  u32 total_tuple_count() const { return tuple_count_; }

 private:
  friend class TableVectorIterator;

  // The empty constructor and Setup() functions are available only to
  // TableVectorIterator because it does lazy initialization

  /// Empty/uninitialized constructor
  VectorProjection();

  /// Set up this vector projection with the given column information and size
  void Setup(const std::vector<const Schema::ColumnInfo *> &col_infos,
             u32 size);

 private:
  // Set the deletions bitmap
  void ClearDeletions();

 private:
  // Metadata about each column in the projection
  std::unique_ptr<const Schema::ColumnInfo *[]> column_info_;

  // The array of pointers to column data for all columns in this projection
  std::unique_ptr<byte *[]> column_data_;

  // The array of pointers to column NULL bitmaps for all columns in this
  // projection
  std::unique_ptr<u32 *[]> column_null_bitmaps_;

  // A bitmap tracking which tuples have been marked for deletion
  util::BitVector deletions_;

  // The number of active tuples
  u32 tuple_count_;

  // The maximum supported size of input tuple vectors
  u32 vector_size_;
};

}  // namespace tpl::sql
