#include "execution/sql/join_hash_table_vector_lookup.h"

#include "execution/sql/join_hash_table.h"

namespace tpl::sql {

JoinHashTableVectorLookup::JoinHashTableVectorLookup(const JoinHashTable &table) noexcept
    : table_(table), match_idx_(0), hashes_{0}, entries_{nullptr} {}

void JoinHashTableVectorLookup::Prepare(VectorProjectionIterator *vpi, const HashFn hash_fn) noexcept {
  TPL_ASSERT(vpi->num_selected() <= kDefaultVectorSize, "VectorProjection size must be less than kDefaultVectorSize");
  // Set up
  match_idx_ = 0;

  // Compute the hashes
  {
    u32 idx = 0;
    vpi->ForEach([this, vpi, hash_fn, &idx]() { hashes_[idx++] = hash_fn(vpi); });
  }

  // Perform the initial lookup
  table_.LookupBatch(vpi->num_selected(), hashes_, entries_);
}

}  // namespace tpl::sql
