#include "execution/sql/column_vector_iterator.h"

#include <algorithm>

#include "execution/sql/column_segment.h"

namespace tpl::sql {

ColumnVectorIterator::ColumnVectorIterator(const Schema::ColumnInfo *col_info) noexcept
    : col_info_(col_info),
      column_(nullptr),
      current_block_pos_(0),
      next_block_pos_(0),
      col_data_(nullptr),
      col_null_bitmap_(nullptr) {}

bool ColumnVectorIterator::Advance() noexcept {
  if (column_ == nullptr || next_block_pos_ == column_->num_tuples()) {
    return false;
  }

  u32 next_elem_offset = next_block_pos_ * col_info_->StorageSize();

  col_data_ = const_cast<byte *>(column_->AccessRaw(next_elem_offset));
  col_null_bitmap_ = const_cast<u32 *>(column_->AccessRawNullBitmap(0));

  current_block_pos_ = next_block_pos_;
  next_block_pos_ = std::min(column_->num_tuples(), current_block_pos_ + vector_size());

  return true;
}

void ColumnVectorIterator::Reset(const ColumnSegment *column) noexcept {
  TPL_ASSERT(column != nullptr, "Cannot reset iterator with NULL block");
  column_ = column;

  // Setup the column data and null data pointers
  col_data_ = const_cast<byte *>(column->AccessRaw(0));
  col_null_bitmap_ = const_cast<u32 *>(column->AccessRawNullBitmap(0));

  // Setup the current position (0) and the next position (the minimum of the
  // length of the column or one vector's length of data)
  current_block_pos_ = 0;
  next_block_pos_ = std::min(column->num_tuples(), vector_size());
}

}  // namespace tpl::sql
