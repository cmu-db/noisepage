#include "common/container/concurrent_bitmap.h"
#include "storage/tuple_access_strategy.h"

namespace terrier::storage {
void TupleAccessStrategy::InitializeRawBlock(RawBlock *raw,
                        const layout_version_t layout_version) {
  // Intentional unsafe cast
  raw->layout_version_ = layout_version;
  raw->num_records_ = 0;
  auto *result = reinterpret_cast<TupleAccessStrategy::Block *>(raw);
  result->NumSlots() = layout_.num_slots_;
  // TODO(Tianyu): For now, columns start right after the header without
  // alignment considerations. This logic will need to change when switching
  // to LLVM.
  uint32_t acc_offset = layout_.header_size_;
  uint32_t *offsets = result->AttrOffets();
  for (uint16_t i = 0; i < layout_.num_cols_; i++) {
    offsets[i] = acc_offset;
    uint32_t column_size = layout_.attr_sizes_[i] * layout_.num_slots_
        + common::BitmapSize(layout_.num_slots_);
    acc_offset += column_size;
  }

  result->NumAttrs(layout_) = layout_.num_cols_;

  for (uint16_t i = 0; i < layout_.num_cols_; i++)
    result->AttrSizes(layout_)[i] = layout_.attr_sizes_[i];
}
}  // namespace storage
