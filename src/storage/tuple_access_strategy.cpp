#include "common/concurrent_bitmap.h"
#include "storage/tuple_access_strategy.h"

namespace terrier {
namespace storage {
namespace {
uint32_t ColumnSize(const BlockLayout &layout,
                    uint16_t col_id) {
  return layout.attr_sizes_[col_id] * layout.num_slots_
      + common::BitmapSize(layout.num_slots_);
}
}

void InitializeRawBlock(RawBlock *raw,
                        const BlockLayout &layout,
                        layout_version_t layout_version) {
  // Intentional unsafe cast
  raw->layout_version_ = layout_version;
  raw->num_records_ = 0;
  auto *result = reinterpret_cast<Block *>(raw);
  result->NumSlots() = layout.num_slots_;
  // TODO(Tianyu): For now, columns start right after the header without
  // alignment considerations. This logic will need to change when switching
  // to LLVM.
  uint32_t acc_offset = layout.header_size_;
  uint32_t *offsets = result->AttrOffets();
  for (uint16_t i = 0; i < layout.num_cols_; i++) {
    offsets[i] = acc_offset;
    acc_offset += ColumnSize(layout, i);
  }

  result->NumAttrs(layout) = layout.num_cols_;

  for (uint16_t i = 0; i < layout.num_cols_; i++)
    result->AttrSizes(layout)[i] = layout.attr_sizes_[i];
}
}
}
