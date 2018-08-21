#include <utility>
#include "common/container/concurrent_bitmap.h"
#include "storage/tuple_access_strategy.h"

namespace terrier::storage {

TupleAccessStrategy::TupleAccessStrategy(BlockLayout layout)
    : layout_(std::move(layout)), column_offsets_(layout.num_cols_) {
  // Calculate the start position of each column
  // we use 64-bit vectorized scans on bitmaps.
  uint32_t acc_offset = StorageUtil::PadUpToSize(sizeof(uint64_t), layout_.header_size_);
  for (uint16_t i = 0; i < layout_.num_cols_; i++) {
    column_offsets_[i] = acc_offset;
    uint32_t column_size = layout_.attr_sizes_[i] * layout_.num_slots_  // content
        + StorageUtil::PadUpToSize(layout_.attr_sizes_[i],
                                   common::BitmapSize(layout_.num_slots_));  // padded-bitmap size
    acc_offset += StorageUtil::PadUpToSize(sizeof(uint64_t), column_size);
  }
}

void TupleAccessStrategy::InitializeRawBlock(RawBlock *const raw,
                                             const layout_version_t layout_version) const {
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

bool TupleAccessStrategy::Allocate(RawBlock *const block, TupleSlot *const slot) const {
  common::RawConcurrentBitmap *bitmap = ColumnNullBitmap(block, PRESENCE_COLUMN_ID);
  const uint32_t start = block->num_records_;

  if (start == layout_.num_slots_) return false;

  uint32_t pos = start;

  while (bitmap->FirstUnsetPos(layout_.num_slots_, pos, &pos)) {
    if (bitmap->Flip(pos, false)) {
      *slot = TupleSlot(block, pos);
      block->num_records_++;
      return true;
    }
  }

  return false;
}
}  // namespace terrier::storage
