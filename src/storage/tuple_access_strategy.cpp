#include "storage/tuple_access_strategy.h"
#include <utility>
#include "common/container/concurrent_bitmap.h"

namespace terrier::storage {

TupleAccessStrategy::TupleAccessStrategy(BlockLayout layout)
    : layout_(std::move(layout)), column_offsets_(layout_.NumColumns()) {
  // Calculate the start position of each column
  // we use 64-bit vectorized scans on bitmaps.
  uint32_t acc_offset = layout_.HeaderSize();
  TERRIER_ASSERT(acc_offset % sizeof(uint64_t) == 0, "size of a header should already be padded to aligned to 8 bytes");
  for (uint16_t i = 0; i < layout_.NumColumns(); i++) {
    column_offsets_[i] = acc_offset;
    uint32_t column_size =
        layout_.AttrSize(col_id_t(i)) * layout_.NumSlots()  // content
        + StorageUtil::PadUpToSize(layout_.AttrSize(col_id_t(i)),
                                   common::RawBitmap::SizeInBytes(layout_.NumSlots()));  // padded-bitmap size
    acc_offset += StorageUtil::PadUpToSize(sizeof(uint64_t), column_size);
  }
}

void TupleAccessStrategy::InitializeRawBlock(RawBlock *const raw, const layout_version_t layout_version) const {
  // Intentional unsafe cast
  raw->layout_version_ = layout_version;
  raw->insert_head_ = 0;
  auto *result = reinterpret_cast<TupleAccessStrategy::Block *>(raw);
  result->NumSlots() = layout_.NumSlots();

  for (uint16_t i = 0; i < layout_.NumColumns(); i++) result->AttrOffets()[i] = column_offsets_[i];

  result->NumAttrs(layout_) = layout_.NumColumns();

  for (uint16_t i = 0; i < layout_.NumColumns(); i++) result->AttrSizes(layout_)[i] = layout_.AttrSize(col_id_t(i));

  result->SlotAllocationBitmap(layout_)->UnsafeClear(layout_.NumSlots());
  result->Column(VERSION_POINTER_COLUMN_ID)->NullBitmap()->UnsafeClear(layout_.NumSlots());
}

bool TupleAccessStrategy::Allocate(RawBlock *const block, TupleSlot *const slot) const {
  common::RawConcurrentBitmap *bitmap = reinterpret_cast<Block *>(block)->SlotAllocationBitmap(layout_);
  const uint32_t start = block->insert_head_;

  // We are not allowed to insert into this block any more
  if (start == layout_.NumSlots()) return false;

  uint32_t pos = start;

  while (bitmap->FirstUnsetPos(layout_.NumSlots(), pos, &pos)) {
    if (bitmap->Flip(pos, false)) {
      *slot = TupleSlot(block, pos);
      block->insert_head_++;
      return true;
    }
  }

  return false;
}
}  // namespace terrier::storage
