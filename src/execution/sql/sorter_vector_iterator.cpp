#include <algorithm>
#include <memory>
#include <vector>

#include "execution/sql/sorter.h"
#include "execution/sql/vector_projection.h"
#include "execution/sql/vector_projection_iterator.h"

namespace noisepage::execution::sql {

SorterVectorIterator::SorterVectorIterator(const Sorter &sorter,
                                           const std::vector<const catalog::Schema::Column *> &column_info,
                                           const SorterVectorIterator::TransposeFn transpose_fn)
    : memory_(sorter.memory_),
      iter_(sorter),
      temp_rows_(memory_->AllocateArray<const byte *>(common::Constants::K_DEFAULT_VECTOR_SIZE, false)),
      vector_projection_(std::make_unique<VectorProjection>()),
      vector_projection_iterator_(std::make_unique<VectorProjectionIterator>()) {
  // First, initialize the vector projection
  std::vector<TypeId> col_types;
  col_types.reserve(column_info.size());
  for (const auto *col_info : column_info) {
    col_types.emplace_back(GetTypeId(col_info->Type()));
  }
  vector_projection_->Initialize(col_types);

  // Now, move the iterator to the next valid position
  Next(transpose_fn);
}

SorterVectorIterator::SorterVectorIterator(const Sorter &sorter, const catalog::Schema::Column *column_info,
                                           uint32_t num_cols, const SorterVectorIterator::TransposeFn transpose_fn)
    : SorterVectorIterator(sorter, {column_info, column_info + num_cols}, transpose_fn) {}

SorterVectorIterator::~SorterVectorIterator() {
  memory_->DeallocateArray(temp_rows_, common::Constants::K_DEFAULT_VECTOR_SIZE);
}

bool SorterVectorIterator::HasNext() const { return vector_projection_->GetSelectedTupleCount() > 0; }

void SorterVectorIterator::Next(const SorterVectorIterator::TransposeFn transpose_fn) {
  // Pull rows into temporary array
  uint32_t size = std::min(iter_.NumRemaining(), static_cast<uint64_t>(common::Constants::K_DEFAULT_VECTOR_SIZE));
  for (uint32_t i = 0; i < size; ++i, ++iter_) {
    temp_rows_[i] = iter_.GetRow();
  }

  // Setup vector projection
  vector_projection_->Reset(size);

  // Build the vector projection
  if (size > 0) {
    BuildVectorProjection(transpose_fn);
  }
}

void SorterVectorIterator::BuildVectorProjection(const SorterVectorIterator::TransposeFn transpose_fn) {
  // Update the vector projection iterator
  vector_projection_iterator_->SetVectorProjection(vector_projection_.get());

  // Invoke the transposition function which does the heavy, query-specific,
  // lifting of converting rows to columns.
  transpose_fn(temp_rows_, vector_projection_->GetSelectedTupleCount(), vector_projection_iterator_.get());

  // The vector projection is now filled with sorted rows in columnar format.
  // Reset the VPI so that it's ready for iteration.
  NOISEPAGE_ASSERT(!vector_projection_iterator_->IsFiltered(), "VPI shouldn't be filtered during a transpose");
  vector_projection_iterator_->Reset();

  // Sanity check
  vector_projection_->CheckIntegrity();
}

}  // namespace noisepage::execution::sql
