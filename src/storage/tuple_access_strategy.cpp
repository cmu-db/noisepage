#include "common/concurrent_bitmap.h"
#include "storage/tuple_access_strategy.h"

namespace terrier {
namespace storage {
namespace {
uint32_t ColumnSize(const BlockLayout &layout,
                    uint16_t col_offset,
                    uint32_t num_slots) {
  return ByteSize(layout.attr_sizes_[col_offset]) * num_slots
      + BitmapSize(num_slots);
}
}

Block *Block::Initialize(RawBlock *raw,
                         const BlockLayout &layout,
                         block_id_t block_id) {
  auto *result = reinterpret_cast<Block *>(raw);
  result->block_id_ = block_id;
  result->num_records_ = 0;
  // Need to account for extra bitmap structures needed for each attribute.
  // TODO(Tianyu): I am subtracting 1 from this number so we will always have
  // space to pad each individual bitmap to full bytes (every attribute is
  // at least a byte). Somebody can come and fix this later, because I don't
  // feel like thinking about this now.
  uint32_t num_slots = 8 * (Constants::BLOCK_SIZE - HeaderSize(layout))
      / (8 * layout.tuple_size_ + layout.num_attrs_) - 1;

  result->num_slots() = num_slots;

  // the first column starts immediately after the end of the header,
  // there is no need to write down its starting offset.
  uint32_t acc_offset = ColumnSize(layout, 0, num_slots);
  uint32_t *offsets = result->attr_offsets();
  for (uint16_t i = 1; i < layout.num_attrs_; i++) {
    offsets[i - 1] = acc_offset;
    acc_offset += ColumnSize(layout, i, num_slots);
  }

  result->num_attrs(layout) = layout.num_attrs_;

  for (uint16_t i = 0; i < layout.num_attrs_; i++)
    result->attr_sizes(layout)[i] = layout.attr_sizes_[i];
  return result;
}
}
}
