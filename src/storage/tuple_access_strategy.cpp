#include "storage/tuple_access_strategy.h"
#include <utility>
#include "common/container/concurrent_bitmap.h"

namespace terrier::storage {

TupleAccessStrategy::TupleAccessStrategy(BlockLayout layout)
    : layout_(std::move(layout)), column_offsets_(layout_.NumCols()) {
  // Calculate the start position of each column
  // we use 64-bit vectorized scans on bitmaps.
  uint32_t acc_offset = StorageUtil::PadUpToSize(sizeof(uint64_t), layout_.HeaderSize());
  for (uint16_t i = 0; i < layout_.NumCols(); i++) {
    column_offsets_[i] = acc_offset;
    uint32_t column_size =
        layout_.AttrSize(i) * layout_.NumSlots()  // content
        + StorageUtil::PadUpToSize(layout_.AttrSize(i),
                                   common::RawBitmap::SizeInBytes(layout_.NumSlots()));  // padded-bitmap size
    acc_offset += StorageUtil::PadUpToSize(sizeof(uint64_t), column_size);
  }
}

void TupleAccessStrategy::InitializeRawBlock(RawBlock *const raw, const layout_version_t layout_version) const {
  // Intentional unsafe cast
  raw->layout_version_ = layout_version;
  raw->num_records_ = 0;
  auto *result = reinterpret_cast<TupleAccessStrategy::Block *>(raw);
  result->NumSlots() = layout_.NumSlots();

  for (uint16_t i = 0; i < layout_.NumCols(); i++) result->AttrOffets()[i] = column_offsets_[i];

  result->NumAttrs(layout_) = layout_.NumCols();

  for (uint16_t i = 0; i < layout_.NumCols(); i++) result->AttrSizes(layout_)[i] = layout_.AttrSize(i);

  result->Column(PRESENCE_COLUMN_ID)->PresenceBitmap()->UnsafeClear(layout_.NumSlots());
}

bool TupleAccessStrategy::Allocate(RawBlock *const block, TupleSlot *const slot) const {
  common::RawConcurrentBitmap *bitmap = ColumnNullBitmap(block, PRESENCE_COLUMN_ID);
  const uint32_t start = block->num_records_;

  if (start == layout_.NumSlots()) return false;

  uint32_t pos = start;

  while (bitmap->FirstUnsetPos(layout_.NumSlots(), pos, &pos)) {
    if (bitmap->Flip(pos, false)) {
      *slot = TupleSlot(block, pos);
      block->num_records_++;
      return true;
    }
  }

  return false;
}
}  // namespace terrier::storage
