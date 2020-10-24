#include <memory>
#include <utility>
#include <vector>

#include "execution/sql/aggregation_hash_table.h"
#include "execution/sql/vector_projection.h"
#include "execution/sql/vector_projection_iterator.h"

namespace noisepage::execution::sql {

AHTVectorIterator::AHTVectorIterator(const AggregationHashTable &agg_hash_table,
                                     const std::vector<const catalog::Schema::Column *> &column_info,
                                     const AHTVectorIterator::TransposeFn transpose_fn)
    : memory_(agg_hash_table.memory_),
      iter_(agg_hash_table.hash_table_, memory_),
      vector_projection_(std::make_unique<VectorProjection>()),
      vector_projection_iterator_(std::make_unique<VectorProjectionIterator>()) {
  // First, initialize the vector projection.
  std::vector<TypeId> col_types;
  col_types.reserve(column_info.size());
  for (const auto *col_info : column_info) {
    col_types.emplace_back(GetTypeId(col_info->Type()));
  }
  vector_projection_->Initialize(col_types);

  // If the iterator has data, build up the projection and the current input.
  if (iter_.HasNext()) {
    BuildVectorProjection(transpose_fn);
  }
}

AHTVectorIterator::AHTVectorIterator(const AggregationHashTable &agg_hash_table,
                                     const catalog::Schema::Column *column_info, const uint32_t num_cols,
                                     const AHTVectorIterator::TransposeFn transpose_fn)
    : AHTVectorIterator(agg_hash_table, {column_info, column_info + num_cols}, transpose_fn) {}

void AHTVectorIterator::BuildVectorProjection(const AHTVectorIterator::TransposeFn transpose_fn) {
  // Pull out payload pointers from hash table entries into our temporary array.
  auto [size, entries] = iter_.GetCurrentBatch();

  // Update the vector projection with the new batch size.
  vector_projection_->Reset(size);
  vector_projection_iterator_->SetVectorProjection(vector_projection_.get());

  // If there isn't data, exit.
  if (size == 0) {
    return;
  }

  // Invoke the transposition function. After the call, row-wise aggregates stored in the temporary
  // aggregate buffer will be converted into column-wise data in vectors within the projection.
  transpose_fn(entries, vector_projection_->GetSelectedTupleCount(), vector_projection_iterator_.get());

  // The vector projection is now filled with vector aggregate data. Reset the VPI so that it's
  // ready for iteration.
  NOISEPAGE_ASSERT(!vector_projection_iterator_->IsFiltered(), "VPI shouldn't be filtered during a transpose");
  vector_projection_iterator_->Reset();

  // Sanity check
  vector_projection_->CheckIntegrity();
}

void AHTVectorIterator::Next(AHTVectorIterator::TransposeFn transpose_fn) {
  NOISEPAGE_ASSERT(HasNext(), "Iterator does not have more data");
  iter_.Next();
  BuildVectorProjection(transpose_fn);
}

}  // namespace noisepage::execution::sql
