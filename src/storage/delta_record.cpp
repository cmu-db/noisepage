#include <vector>
#include "storage/delta_record.h"
#include "storage/storage_util.h"

namespace terrier::storage {
uint32_t ProjectedRow::Size(const BlockLayout &layout, const std::vector<uint16_t> &col_ids) {
  uint32_t result = sizeof(ProjectedRow);  // size and num_col size
  for (uint16_t col_id : col_ids)
    result += static_cast<uint32_t>(sizeof(uint16_t) + sizeof(uint32_t) + layout.attr_sizes_[col_id]);
  result += common::BitmapSize(static_cast<uint32_t>(col_ids.size()));
  return storage::StorageUtil::PadUpToSize(sizeof(uint64_t), result);  // pad up to 8 bytes
}

ProjectedRow *ProjectedRow::InitializeProjectedRow(void *head,
                                                   const std::vector<uint16_t> &col_ids,
                                                   const BlockLayout &layout) {
  auto *result = reinterpret_cast<ProjectedRow *>(head);
  // TODO(Tianyu): This is redundant calculation
  result->size_ = Size(layout, col_ids);
  result->num_cols_ = static_cast<uint16_t>(col_ids.size());
  auto val_offset =
      static_cast<uint32_t>(sizeof(ProjectedRow) + result->num_cols_ * (sizeof(uint16_t) + sizeof(uint32_t)) +
          common::BitmapSize(result->num_cols_));
  for (uint16_t i = 0; i < col_ids.size(); i++) {
    result->ColumnIds()[i] = col_ids[i];
    result->AttrValueOffsets()[i] = val_offset;
    val_offset += layout.attr_sizes_[col_ids[i]];
  }
  result->Bitmap().Clear(result->num_cols_);
  return result;
}

ProjectedRow* ProjectedRow::InitializeProjectedRow(void *head, const ProjectedRow &other)  {
  auto *result = reinterpret_cast<ProjectedRow *>(head);
  auto header_size =
      static_cast<uint32_t>(sizeof(ProjectedRow) + +other.num_cols_ * (sizeof(uint16_t) + sizeof(uint32_t)));
  PELOTON_MEMCPY(result, &other, header_size);
  result->Bitmap().Clear(result->num_cols_);
  return result;
}

UndoRecord *UndoRecord::InitializeRecord(void *head,
                                         timestamp_t timestamp,
                                         TupleSlot slot,
                                         DataTable *table,
                                         const BlockLayout &layout,
                                         const std::vector<uint16_t> &col_ids) {
  auto *result = reinterpret_cast<UndoRecord *>(head);

  result->next_ = nullptr;
  result->timestamp_.store(timestamp);
  result->table_ = table;
  result->slot_ = slot;

  ProjectedRow::InitializeProjectedRow(result->varlen_contents_, col_ids, layout);

  return result;
}
}  // namespace terrier::storage
