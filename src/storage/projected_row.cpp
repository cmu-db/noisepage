#include "storage/projected_row.h"
#include <algorithm>
#include <cstring>
#include <functional>
#include <numeric>
#include <set>
#include <utility>
#include <vector>

namespace terrier::storage {
ProjectedRow *ProjectedRow::CopyProjectedRowLayout(void *head, const ProjectedRow &other) {
  auto *result = reinterpret_cast<ProjectedRow *>(head);
  auto header_size = reinterpret_cast<uintptr_t>(&other.Bitmap()) - reinterpret_cast<uintptr_t>(&other);
  std::memcpy(result, &other, header_size);
  result->Bitmap().Clear(result->num_cols_);
  return result;
}

// TODO(Tianyu): I don't think we can reasonably fit these into a cache line?
ProjectedRowInitializer::ProjectedRowInitializer(PRInitCreator creator)
    : col_ids_(std::move(creator.col_ids)), offsets_(col_ids_.size()) {
  TERRIER_ASSERT(!col_ids_.empty(), "Cannot initialize an empty ProjectedRow.");
  TERRIER_ASSERT(col_ids_.size() == creator.attr_sizes.size(),
                 "Attribute sizes should correspond to the column indexes");
  TERRIER_ASSERT(std::is_sorted(creator.attr_sizes.cbegin(), creator.attr_sizes.cend(), std::greater<>()),
                 "Attribute sizes must be sorted descending.");
  TERRIER_ASSERT(std::is_sorted(col_ids_.cbegin(), col_ids_.cend(), std::less<>()),
                 "Column ids must be sorted ascending.");
  TERRIER_ASSERT((std::set<col_id_t>(col_ids_.cbegin(), col_ids_.cend())).size() == col_ids_.size(),
                 "There should not be any duplicates in the col_ids.");
  // TODO(Tianyu): We should really assert that it has a subset of columns, but that is a bit more complicated.

  size_ = sizeof(ProjectedRow);  // size and num_col size
  // space needed to store col_ids, must be padded up so that the following offsets are aligned
  size_ = StorageUtil::PadUpToSize(sizeof(uint32_t), size_ + static_cast<uint32_t>(col_ids_.size() * sizeof(uint16_t)));
  // space needed to store value offsets, we don't need to pad as we're using a regular non-concurrent bitmap
  size_ = size_ + static_cast<uint32_t>(col_ids_.size() * sizeof(uint32_t));
  // Pad up to either the first value's size, or 8 bytes if the value is larger than 8
  uint8_t first_alignment = creator.attr_sizes[0];
  if (first_alignment > sizeof(uint64_t)) first_alignment = sizeof(uint64_t);
  // space needed to store the bitmap, padded up to the size of the first value in this projected row
  size_ = StorageUtil::PadUpToSize(first_alignment,
                                   size_ + common::RawBitmap::SizeInBytes(static_cast<uint32_t>(col_ids_.size())));
  for (uint32_t i = 0; i < col_ids_.size(); i++) {
    offsets_[i] = size_;
    // Pad up to either the next value's size, or 8 bytes at the end of the ProjectedRow, or 8 byte if the value
    // is larger than 8
    auto next_alignment = static_cast<uint8_t>(i == col_ids_.size() - 1 ? sizeof(uint64_t) : creator.attr_sizes[i + 1]);
    if (next_alignment > sizeof(uint64_t)) next_alignment = sizeof(uint64_t);
    size_ = StorageUtil::PadUpToSize(next_alignment, size_ + creator.attr_sizes[i]);
  }
}

ProjectedRow *ProjectedRowInitializer::InitializeRow(void *const head) const {
  TERRIER_ASSERT(reinterpret_cast<uintptr_t>(head) % sizeof(uint64_t) == 0,
                 "start of ProjectedRow needs to be aligned to 8 bytes to"
                 "ensure correctness of alignment of its members");
  auto *result = reinterpret_cast<ProjectedRow *>(head);
  result->size_ = size_;
  result->num_cols_ = static_cast<uint16_t>(col_ids_.size());
  for (uint32_t i = 0; i < col_ids_.size(); i++) result->ColumnIds()[i] = col_ids_[i];
  for (uint32_t i = 0; i < col_ids_.size(); i++) result->AttrValueOffsets()[i] = offsets_[i];
  result->Bitmap().Clear(result->num_cols_);
  return result;
}

ProjectedRowInitializer::PRInitCreator ProjectedRowInitializer::PreparePRInit(const BlockLayout &layout,
                                                                              std::vector<col_id_t> col_ids) {
  TERRIER_ASSERT(col_ids.size() < layout.NumColumns(),
                 "ProjectedRow should have fewer columns than the table (can't read version vector)");
  // Sort the projection list for optimal space utilization and delta application performance
  // If the col ids are valid ones laid out by BlockLayout, ascending order of id guarantees
  // descending order in attribute size.
  std::sort(col_ids.begin(), col_ids.end(), std::less<>());
  std::vector<uint8_t> attr_sizes;
  attr_sizes.reserve(col_ids.size());
  for (auto const &col_id : col_ids) {
    attr_sizes.emplace_back(layout.AttrSize(col_id));
  }
  return {std::move(attr_sizes), std::move(col_ids)};
}

ProjectedRowInitializer::PRInitCreator ProjectedRowInitializer::PreparePRInit(std::vector<uint8_t> attr_sizes) {
  TERRIER_ASSERT(std::is_sorted(attr_sizes.cbegin(), attr_sizes.cend(), std::greater<>()),
                 "Attribute sizes must be sorted descending");
  // the col_ids we generate must be ascending
  std::vector<col_id_t> col_ids;
  col_ids.reserve(attr_sizes.size());
  for (uint32_t i = 0; i < attr_sizes.size(); i++) {
    col_ids.emplace_back(i);
  }
  return {std::move(attr_sizes), std::move(col_ids)};
}
}  // namespace terrier::storage
