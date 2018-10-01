#include "storage/projected_columns.h"
#include <algorithm>
#include <functional>
#include <utility>
#include <vector>
namespace terrier::storage {
ProjectedColumnsInitializer::ProjectedColumnsInitializer(const terrier::storage::BlockLayout &layout,
                                                         std::vector<terrier::col_id_t> col_ids, uint32_t max_tuples)
    : max_tuples_(max_tuples), col_ids_(std::move(col_ids)), offsets_(col_ids_.size()) {
  TERRIER_ASSERT(!col_ids_.empty(), "cannot initialize an empty ProjectedColumns");
  TERRIER_ASSERT(col_ids_.size() < layout.NumColumns(),
                 "ProjectedColumns should have fewer columns than the table (can't read version vector)");
  // TODO(Tianyu): We should really assert that it has a subset of columns, but that is a bit more complicated.

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

  for (uint32_t i = 0; i < col_ids_.size(); i++) {
    offsets_[i] = size_;
    // space needed to store the bitmap, padded up to 8 bytes
    size_ = StorageUtil::PadUpToSize(sizeof(uint64_t),
                                     size_ + common::RawBitmap::SizeInBytes(static_cast<uint32_t>(max_tuples_)));
    // Pad up to always 8 bytes across columns.
    size_ = StorageUtil::PadUpToSize(sizeof(uint64_t), size_ + layout.AttrSize(col_ids_[i]) * max_tuples_);
  }
}

ProjectedColumns *ProjectedColumnsInitializer::Initialize(void *head) const {
  TERRIER_ASSERT(reinterpret_cast<uintptr_t>(head) % sizeof(uint64_t) == 0,
                 "start of ProjectedRow needs to be aligned to 8 bytes to"
                 "ensure correctness of alignment of its members");
  auto *result = reinterpret_cast<ProjectedColumns *>(head);
  result->size_ = size_;
  result->max_tuples_ = max_tuples_;
  result->num_tuples_ = 0;
  result->num_cols_ = static_cast<uint16_t>(col_ids_.size());
  for (uint32_t i = 0; i < col_ids_.size(); i++) result->ColumnIds()[i] = col_ids_[i];
  for (uint32_t i = 0; i < col_ids_.size(); i++) result->AttrValueOffsets()[i] = offsets_[i];
  // No need to initialize the rest since we made it clear 0 tuples currently are in the buffer
  return result;
}
}  // namespace terrier::storage
