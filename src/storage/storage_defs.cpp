#include <vector>

#include "storage/storage_defs.h"

namespace terrier::storage {
uint32_t ProjectedRow::RowSize(const BlockLayout &layout, const std::vector<uint16_t> &col_ids)  {
  uint32_t result = sizeof(uint16_t);  // num_col size
  for (uint16_t col_id : col_ids)
    result += static_cast<uint32_t>(sizeof(uint16_t) + sizeof(uint32_t) + layout.attr_sizes_[col_id]);
  return result + common::BitmapSize(static_cast<uint32_t>(col_ids.size()));
}

ProjectedRow* ProjectedRow::InitializeProjectedRow(const BlockLayout &layout,
                                                   const std::vector<uint16_t> &col_ids,
                                                   byte *head) {
  auto *result = reinterpret_cast<ProjectedRow *>(head);
  result->num_cols_ = static_cast<uint16_t>(col_ids.size());
  auto val_offset =
      static_cast<uint32_t>(sizeof(uint16_t) + result->num_cols_ * (sizeof(uint16_t) + sizeof(uint32_t)) +
          common::BitmapSize(result->num_cols_));
  for (uint16_t i = 0; i < col_ids.size(); i++) {
    result->ColumnIds()[i] = col_ids[i];
    result->AttrValueOffsets()[i] = val_offset;
    val_offset += layout.attr_sizes_[col_ids[i]];
  }
  result->Bitmap().Clear(result->num_cols_);
  return result;
}
}  // namespace terrier::storage
