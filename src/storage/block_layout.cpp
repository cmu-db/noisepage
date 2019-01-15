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
  for (uint8_t size UNUSED_ATTRIBUTE : attr_sizes_)
    TERRIER_ASSERT(size == VARLEN_COLUMN || (size >= 0 && size <= INT8_MAX), "Invalid size of a column");
  TERRIER_ASSERT(!attr_sizes_.empty() && static_cast<uint16_t>(attr_sizes_.size()) <= common::Constants::MAX_COL,
                 "number of columns must be between 1 and 32767");
  TERRIER_ASSERT(num_slots_ != 0, "number of slots cannot be 0!");
  // sort the attributes when laying out memory to minimize impact of padding
  // This is always safe because we know there are at last 2 columns
  std::sort(attr_sizes_.begin() + 1, attr_sizes_.end(), std::greater<>());
  for (uint32_t i = 0; i < attr_sizes_.size(); i++)
    if (attr_sizes_[i] == VARLEN_COLUMN) varlens_.emplace_back(i);
}

uint32_t BlockLayout::ComputeTupleSize() const {
  uint32_t result = 0;
  // size in attr_sizes_ can be negative to denote varlens.
  for (auto size : attr_sizes_) result += static_cast<uint8_t>(INT8_MAX & size);
  return result;
}

uint32_t BlockLayout::ComputeStaticHeaderSize() const {
  auto unpadded_size =
      static_cast<uint32_t>(sizeof(uint32_t) * 3  // layout_version, num_records, num_slots
                            + NumColumns() * sizeof(uint32_t) + sizeof(uint16_t) + NumColumns() * sizeof(uint8_t));
  return StorageUtil::PadUpToSize(sizeof(uint64_t), unpadded_size);
}

uint32_t BlockLayout::ComputeNumSlots() const {
  // TODO(Tianyu):
  // We will have to subtract 8 bytes maximum padding for each column's bitmap. Subtracting another 1 to account for
  // the padding at the end of each column. Somebody can come and fix
  // this later, because I don't feel like thinking about this now.
  return 8 * (common::Constants::BLOCK_SIZE - static_header_size_ - 2 * 8 * NumColumns()) /
         (8 * tuple_size_ + NumColumns() + 1);
}

uint32_t BlockLayout::ComputeHeaderSize() const {
  return StorageUtil::PadUpToSize(sizeof(uint64_t), static_header_size_ + common::RawBitmap::SizeInBytes(NumSlots()));
}

}  // namespace terrier::storage
