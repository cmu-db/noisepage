#pragma once

#include "execution/sql/schema.h"

namespace tpl::sql {

class ColumnSegment;

/// An iterator over the in-memory contents of a column's data. This iterator
/// performs an iteration over column data in vector-at-a-time fashion. Each
/// iteration returns at most \a vector_size() tuples (less if the column has
/// fewer than \a vector_size() tuples remaining).
class ColumnVectorIterator {
 public:
  explicit ColumnVectorIterator(const Schema::ColumnInfo *col_info) noexcept;

  /// Advance this iterator to the next vector of input in the column
  /// \return True if there is more data in the iterator; false otherwise
  bool Advance() noexcept;

  /// The number of tuples in the input. Or, in other words, the number of
  /// elements in the array returned by \a col_data() or \a col_null_bitmap()
  /// \return The number of tuples in the currently active vector
  u32 NumTuples() const { return next_block_pos_ - current_block_pos_; }

  /// Reset the iterator to begin iteration at the start \a column
  /// \param column The column to begin iteration over
  void Reset(const ColumnSegment *column) noexcept;

  // -------------------------------------------------------
  // Accessors
  // -------------------------------------------------------

  static constexpr u32 vector_size() { return kDefaultVectorSize; }

  /// Access the current vector of raw untyped column data
  byte *col_data() noexcept { return col_data_; }
  byte *col_data() const noexcept { return col_data_; }

  /// Access the current raw NULL bitmap
  u32 *col_null_bitmap() noexcept { return col_null_bitmap_; }
  u32 *col_null_bitmap() const noexcept { return col_null_bitmap_; }

 private:
  // The schema information for the column this iterator operates on
  const Schema::ColumnInfo *col_info_;

  // The segment we're currently iterating over
  const ColumnSegment *column_;

  // The current position in the current block
  u32 current_block_pos_;

  // The position in the current block to find the next vector of input
  u32 next_block_pos_;

  // Pointer to column data
  byte *col_data_;

  // Pointer to the column's bitmap
  u32 *col_null_bitmap_;
};

}  // namespace tpl::sql
