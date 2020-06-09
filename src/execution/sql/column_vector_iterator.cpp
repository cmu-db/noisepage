#include "execution/sql/column_vector_iterator.h"

#include <algorithm>

#include "catalog/schema.h"
#include "execution/sql/column_segment.h"

namespace terrier::execution::sql {

ColumnVectorIterator::ColumnVectorIterator(uint32_t column_attr_size) noexcept
    : column_attr_size_(column_attr_size),
      column_(nullptr),
      current_block_pos_(0),
      next_block_pos_(0),
      col_data_(nullptr),
      col_null_bitmap_(nullptr) {}

bool ColumnVectorIterator::Advance() noexcept {
  if (column_ == nullptr || next_block_pos_ == column_->GetTupleCount()) {
    return false;
  }

  uint32_t next_elem_offset = next_block_pos_ * column_attr_size_;

  col_data_ = const_cast<byte *>(column_->AccessRaw(next_elem_offset));
  col_null_bitmap_ = const_cast<uint32_t *>(column_->AccessRawNullBitmap(0));

  current_block_pos_ = next_block_pos_;
  next_block_pos_ = std::min(column_->GetTupleCount(), current_block_pos_ + common::Constants::K_DEFAULT_VECTOR_SIZE);

  return true;
}

void ColumnVectorIterator::Reset(const ColumnSegment *column) noexcept {
  TERRIER_ASSERT(column != nullptr, "Cannot reset iterator with NULL block");
  column_ = column;

  // Setup the column data and null data pointers
  col_data_ = const_cast<byte *>(column->AccessRaw(0));
  col_null_bitmap_ = const_cast<uint32_t *>(column->AccessRawNullBitmap(0));

  // Setup the current position (0) and the next position
  // (the minimum of the length of the column or one vector's length of data)
  current_block_pos_ = 0;
  next_block_pos_ = std::min(column->GetTupleCount(), common::Constants::K_DEFAULT_VECTOR_SIZE);
}

}  // namespace terrier::execution::sql
