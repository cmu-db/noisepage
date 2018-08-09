#include <utility>
#include "common/container/concurrent_bitmap.h"
#include "storage/tuple_access_strategy.h"

namespace terrier::storage {

TupleAccessStrategy::TupleAccessStrategy(BlockLayout layout)
    : layout_(std::move(layout)), column_offsets_(layout.num_cols_) {
  // Calculate the start position of each column
  // we use 64-bit vectorized scans on bitmaps.
  uint32_t acc_offset = PadOffsetToSize(sizeof(uint64_t), layout_.header_size_);
  for (uint16_t i = 0; i < layout_.num_cols_; i++) {
    column_offsets_[i] = acc_offset;
    uint32_t column_size = layout_.attr_sizes_[i] * layout_.num_slots_  // content
        + PadOffsetToSize(layout_.attr_sizes_[i], common::BitmapSize(layout_.num_slots_));  // padded-bitmap size
    acc_offset += PadOffsetToSize(sizeof(uint64_t), column_size);
  }
}

void TupleAccessStrategy::InitializeRawBlock(RawBlock *raw,
                                             const layout_version_t layout_version) {
  // Intentional unsafe cast
  raw->layout_version_ = layout_version;
  raw->num_records_ = 0;
  auto *result = reinterpret_cast<TupleAccessStrategy::Block *>(raw);
  result->NumSlots() = layout_.num_slots_;

  for (uint16_t i = 0; i < layout_.num_cols_; i++)
    result->AttrOffets()[i] = column_offsets_[i];

  result->NumAttrs(layout_) = layout_.num_cols_;

  for (uint16_t i = 0; i < layout_.num_cols_; i++)
    result->AttrSizes(layout_)[i] = layout_.attr_sizes_[i];

  result->Column(PRESENCE_COLUMN_ID)->PresenceBitmap()->UnsafeClear(layout_.num_slots_);
}
}  // namespace terrier::storage
