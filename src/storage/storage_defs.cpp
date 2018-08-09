#include <vector>

#include "storage/storage_defs.h"
#include "storage/storage_util.h"

namespace terrier::storage {
uint32_t ProjectedRow::Size(const BlockLayout &layout, const std::vector<uint16_t> &col_ids) {
  uint32_t result = sizeof(uint32_t) + sizeof(uint16_t);  // size and num_col size
  for (uint16_t col_id : col_ids)
    result += static_cast<uint32_t>(sizeof(uint16_t) + sizeof(uint32_t) + layout.attr_sizes_[col_id]);
  return result + common::BitmapSize(static_cast<uint32_t>(col_ids.size()));
}

ProjectedRow *ProjectedRow::InitializeProjectedRow(void *head,
                                                   const std::vector<uint16_t> &col_ids,
                                                   const BlockLayout &layout) {
  auto *result = reinterpret_cast<ProjectedRow *>(head);
  // TODO(Tianyu): This is redundant calculation
  result->size_ = Size(layout, col_ids);
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

DeltaRecord *DeltaRecord::InitializeDeltaRecord(void *head,
                                                timestamp_t timestamp,
                                                const BlockLayout &layout,
                                                const std::vector<uint16_t> &col_ids) {
  auto *result = reinterpret_cast<DeltaRecord *>(head);

  result->next_ = nullptr;
  result->timestamp_ = timestamp;
  // TODO(Tianyu): This is redundant calculation
  result->size_ = Size(layout, col_ids);

  ProjectedRow::InitializeProjectedRow(result->varlen_contents_, col_ids, layout);

  return result;
}

}  // namespace terrier::storage
