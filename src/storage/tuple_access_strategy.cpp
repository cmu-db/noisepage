#include "common/concurrent_bitmap.h"
#include "storage/tuple_access_strategy.h"

namespace terrier {
namespace storage {
namespace {
uint32_t ColumnSize(const BlockLayout &layout,
                    uint16_t col_offset) {
  return layout.attr_sizes_[col_offset] * layout.num_slots_
      + BitmapSize(layout.num_slots_);
}
}

void InitializeRawBlock(RawBlock *raw,
                        const BlockLayout &layout,
                        block_id_t block_id) {
  // Intentional unsafe cast
  auto *result = reinterpret_cast<Block *>(raw);
  result->block_id_ = block_id;
  result->num_records_ = 0;
  result->num_slots() = layout.num_slots_;
  // the first column starts immediately after the end of the header,
  // there is no need to write down its starting offset.
  uint32_t acc_offset = ColumnSize(layout, 0);
  uint32_t *offsets = result->attr_offsets();
  for (uint16_t i = 1; i < layout.num_attrs_; i++) {
    offsets[i - 1] = acc_offset;
    acc_offset += ColumnSize(layout, i);
  }

  result->num_attrs(layout) = layout.num_attrs_;

  for (uint16_t i = 0; i < layout.num_attrs_; i++)
    result->attr_sizes(layout)[i] = layout.attr_sizes_[i];
}
}
}
