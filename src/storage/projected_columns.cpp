#include "storage/projected_columns.h"

#include <algorithm>
#include <functional>
#include <set>
#include <utility>
#include <vector>

#include "storage/block_layout.h"

namespace noisepage::storage {
uint32_t ProjectedColumns::AttrSizeForColumn(const uint16_t projection_col_index) {
  NOISEPAGE_ASSERT(projection_col_index < num_cols_, "Cannot get size for out-of-bounds column");
  uint8_t shift;
  for (shift = 0; shift < NUM_ATTR_BOUNDARIES; shift++) {
    if (projection_col_index < attr_ends_[shift]) break;
  }
  NOISEPAGE_ASSERT(shift <= NUM_ATTR_BOUNDARIES, "Out-of-bounds attribute size");
  NOISEPAGE_ASSERT(shift >= 0, "Out-of-bounds attribute size");
  return 16U >> shift;
}

ProjectedColumnsInitializer::ProjectedColumnsInitializer(const BlockLayout &layout, std::vector<col_id_t> col_ids,
                                                         const uint32_t max_tuples)
    : max_tuples_(max_tuples), col_ids_(std::move(col_ids)), offsets_(col_ids_.size()) {
  NOISEPAGE_ASSERT(!col_ids_.empty(), "cannot initialize an empty ProjectedColumns");
  NOISEPAGE_ASSERT(col_ids_.size() < layout.NumColumns(),
                   "ProjectedColumns should have fewer columns than the table (can't read version vector)");
  NOISEPAGE_ASSERT((std::set<col_id_t>(col_ids_.cbegin(), col_ids_.cend())).size() == col_ids_.size(),
                   "There should not be any duplicated in the col_ids!");

  // Sort the projection list for optimal space utilization and delta application performance
  // If the col ids are valid ones laid out by BlockLayout, ascending order of id guarantees
  // descending order in attribute size.
  std::sort(col_ids_.begin(), col_ids_.end(), std::less<>());

  size_ = sizeof(ProjectedColumns);
  // space needed to store col_ids, must be padded up so that the following offsets are aligned
  size_ = StorageUtil::PadUpToSize(sizeof(uint32_t), size_ + static_cast<uint32_t>(col_ids_.size() * sizeof(uint16_t)));
  // space needed to store value offsets, pad up to 8 bytes to store tuple slots
  size_ =
      StorageUtil::PadUpToSize(sizeof(TupleSlot), size_ + static_cast<uint32_t>(col_ids_.size() * sizeof(uint32_t)));
  // Space needed to store tuple slots, no need to pad bitmaps
  size_ += static_cast<uint32_t>(sizeof(TupleSlot) * max_tuples_);

  int attr_size_index = 0;
  for (auto &attr_end : attr_ends_) attr_end = 0;

  for (uint32_t i = 0; i < col_ids_.size(); i++) {
    NOISEPAGE_ASSERT(i < (1 << 15), "Out-of-bounds index");
    offsets_[i] = size_;

    // Build out the array that stores the boundaries for attribute sizes
    int attr_size = layout.AttrSize(col_ids_[i]);
    NOISEPAGE_ASSERT(attr_size <= (16 >> attr_size_index), "Out-of-order columns");
    NOISEPAGE_ASSERT(attr_size <= 16 && attr_size > 0, "Unexpected attribute size");
    while (attr_size < (16 >> attr_size_index)) {
      if (attr_size_index < (NUM_ATTR_BOUNDARIES - 1)) attr_ends_[attr_size_index + 1] = attr_ends_[attr_size_index];
      attr_size_index++;
    }
    NOISEPAGE_ASSERT(attr_size == (16 >> attr_size_index), "Non-power of two attribute size");
    if (attr_size_index < NUM_ATTR_BOUNDARIES) attr_ends_[attr_size_index]++;
    NOISEPAGE_ASSERT(attr_size_index == NUM_ATTR_BOUNDARIES || attr_ends_[attr_size_index] == i + 1,
                     "Inconsistent state on attribute bounds");

    // space needed to store the bitmap, padded up to 8 bytes
    size_ = StorageUtil::PadUpToSize(sizeof(uint64_t),
                                     size_ + common::RawBitmap::SizeInBytes(static_cast<uint32_t>(max_tuples_)));
    // Pad up to always 8 bytes across columns.
    size_ = StorageUtil::PadUpToSize(sizeof(uint64_t), size_ + attr_size * max_tuples_);
  }
}

ProjectedColumns *ProjectedColumnsInitializer::Initialize(void *const head) const {
  NOISEPAGE_ASSERT(reinterpret_cast<uintptr_t>(head) % sizeof(uint64_t) == 0,
                   "start of ProjectedRow needs to be aligned to 8 bytes to"
                   "ensure correctness of alignment of its members");
  auto *result = reinterpret_cast<ProjectedColumns *>(head);
  result->size_ = size_;
  result->max_tuples_ = max_tuples_;
  result->num_tuples_ = 0;
  for (int i = 0; i < 4; i++) result->attr_ends_[i] = attr_ends_[i];
  result->num_cols_ = static_cast<uint16_t>(col_ids_.size());
  for (uint32_t i = 0; i < col_ids_.size(); i++) result->ColumnIds()[i] = col_ids_[i];
  for (uint32_t i = 0; i < col_ids_.size(); i++) result->AttrValueOffsets()[i] = offsets_[i];
  // No need to initialize the rest since we made it clear 0 tuples currently are in the buffer
  return result;
}
}  // namespace noisepage::storage
