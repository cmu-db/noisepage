#include "execution/sql/join_hash_table_vector_probe.h"
#include "execution/sql/vector_projection_iterator.h"

#include "execution/sql/join_hash_table.h"

namespace terrier::execution::sql {

JoinHashTableVectorProbe::JoinHashTableVectorProbe(const JoinHashTable &table)
    : table_(table), match_idx_(0), hashes_{0}, entries_{nullptr} {}

void JoinHashTableVectorProbe::Prepare(VectorProjectionIterator *vpi, const HashFn hash_fn) {
  TERRIER_ASSERT(vpi->NumSelected() <= common::Constants::K_DEFAULT_VECTOR_SIZE,
                 "ProjectedColumns size must be less than common::Constants::K_DEFAULT_VECTOR_SIZE");
  // Set up
  match_idx_ = 0;

  // Compute the hashes
  if (vpi->IsFiltered()) {
    for (uint32_t idx = 0; vpi->HasNextFiltered(); vpi->AdvanceFiltered()) {
      hashes_[idx++] = hash_fn(vpi);
    }
  } else {
    for (uint32_t idx = 0; vpi->HasNext(); vpi->Advance()) {
      hashes_[idx++] = hash_fn(vpi);
    }
  }

  // Reset the iterator since we just exhausted it from the previous hash
  // computation loop.
  vpi->Reset();

  // Perform the initial lookup
  table_.LookupBatch(vpi->NumSelected(), hashes_, entries_);
}

}  // namespace terrier::execution::sql
