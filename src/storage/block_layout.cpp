#include "storage/block_layout.h"

#include <algorithm>
#include <utility>
#include <vector>

#include "storage/arrow_block_metadata.h"
#include "storage/storage_util.h"

namespace noisepage::storage {
BlockLayout::BlockLayout(std::vector<uint16_t> attr_sizes)
    : attr_sizes_(std::move(attr_sizes)),
      tuple_size_(ComputeTupleSize()),
      static_header_size_(ComputeStaticHeaderSize()),
      num_slots_(ComputeNumSlots()),
      header_size_(ComputeHeaderSize()) {
  for (uint16_t size UNUSED_ATTRIBUTE : attr_sizes_)
    NOISEPAGE_ASSERT(size == VARLEN_COLUMN || (size >= 0 && size <= INT16_MAX), "Invalid size of a column");
  NOISEPAGE_ASSERT(!attr_sizes_.empty() && static_cast<uint16_t>(attr_sizes_.size()) <= common::Constants::MAX_COL,
                   "number of columns must be between 1 and MAX_COL");
  NOISEPAGE_ASSERT(num_slots_ != 0, "number of slots cannot be 0!");
  // sort the attributes when laying out memory to minimize impact of padding
  // skip the reserved columns because we still want those first and shouldn't mess up 8-byte alignment
  std::sort(attr_sizes_.begin() + NUM_RESERVED_COLUMNS, attr_sizes_.end(), std::greater<>());
  for (uint32_t i = 0; i < attr_sizes_.size(); i++)
    if (attr_sizes_[i] == VARLEN_COLUMN) varlens_.emplace_back(i);
}

uint32_t BlockLayout::ComputeTupleSize() const {
  uint32_t result = 0;
  // size in attr_sizes_ can be negative to denote varlens.
  for (auto size : attr_sizes_) result += AttrSizeBytes(size);
  return result;
}

uint32_t BlockLayout::ComputeStaticHeaderSize() const {
  auto unpadded_size = static_cast<uint32_t>(
      sizeof(uintptr_t) + sizeof(uint16_t) + sizeof(layout_version_t) +  // datatable pointer, padding, layout_version
      sizeof(uint32_t)                                                   // insert_head
      + sizeof(BlockAccessController) + ArrowBlockMetadata::Size(NumColumns())  // access controller and metadata
      + NumColumns() * sizeof(uint32_t));                                       // attr_offsets
  return StorageUtil::PadUpToSize(sizeof(uint64_t), unpadded_size);
}

uint32_t BlockLayout::ComputeNumSlots() const {
  uint32_t bytes_available = common::Constants::BLOCK_SIZE - static_header_size_;
  // account for paddings up to 64 bits-aligned. There is padding between every bitmap and value field.
  // Each column has a bitmap and a value buffer. The first column can have padding against header. The
  // last column has nothing to pad to.
  bytes_available -= static_cast<uint32_t>(sizeof(uint64_t)) * 2 * NumColumns();
  // Every column needs a bit for bitmap, plus a global presence bit for the whole tuple
  uint32_t bits_per_tuple = BYTE_SIZE * tuple_size_ + NumColumns() + 1;
  return BYTE_SIZE * bytes_available / bits_per_tuple;
}

uint32_t BlockLayout::ComputeHeaderSize() const {
  return StorageUtil::PadUpToSize(sizeof(uint64_t), static_header_size_ + common::RawBitmap::SizeInBytes(NumSlots()));
}

}  // namespace noisepage::storage
