#include "storage/block_layout.h"
#include <algorithm>
#include <functional>
#include <utility>
#include <vector>
#include "storage/storage_util.h"

namespace terrier::storage {
BlockLayout::BlockLayout(std::vector<uint8_t> attr_sizes)
    : attr_sizes_(std::move(attr_sizes)),
      tuple_size_(ComputeTupleSize()),
      static_header_size_(ComputeStaticHeaderSize()),
      num_slots_(ComputeNumSlots()),
      header_size_(ComputeHeaderSize()) {
  TERRIER_ASSERT(!attr_sizes_.empty() && static_cast<uint16_t>(attr_sizes_.size()) <= common::Constants::MAX_COL,
                 "number of columns must be between 1 and 32767");
  TERRIER_ASSERT(num_slots_ != 0, "number of slots cannot be 0!");
  // sort the attributes when laying out memory to minimize impact of padding
  std::sort(attr_sizes_.begin(), attr_sizes_.end(), std::greater<>());
}

uint32_t BlockLayout::ComputeTupleSize() const {
  uint32_t result = 0;
  for (auto size : attr_sizes_) result += size;
  return result;
}

uint32_t BlockLayout::ComputeStaticHeaderSize() const {
  auto unpadded_size =
      static_cast<uint32_t>(sizeof(uint32_t) * 3 // layout_version, num_records, num_slots
                            + sizeof(BlockAccessController)
                            + NumColumns() * sizeof(uint32_t) + sizeof(uint16_t) + NumColumns() * sizeof(uint8_t));
  return StorageUtil::PadUpToSize(sizeof(uint64_t), unpadded_size);
}

uint32_t BlockLayout::ComputeNumSlots() const {
  // TODO(Tianyu):
  // subtracting 1 from this number so we will always have
  // space to pad each individual bitmap to full bytes (every attribute is
  // at least a byte). Subtracting another 1 to account for padding. Somebody can come and fix
  // this later, because I don't feel like thinking about this now.
  // TODO(Tianyu): Now with sortedness in our layout, we don't necessarily have the worse case where padding can take
  // up to the size of 1 tuple, so this can probably change to be more optimistic,
  return 8 * (common::Constants::BLOCK_SIZE - static_header_size_) / (8 * tuple_size_ + NumColumns() + 1) - 2;
}

uint32_t BlockLayout::ComputeHeaderSize() const {
  return StorageUtil::PadUpToSize(sizeof(uint64_t), static_header_size_ + common::RawBitmap::SizeInBytes(NumSlots()));
}

}  // namespace terrier::storage
