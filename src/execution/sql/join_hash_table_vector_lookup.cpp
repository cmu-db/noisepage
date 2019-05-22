#include "execution/sql/join_hash_table_vector_lookup.h"

#include "execution/sql/join_hash_table.h"

namespace tpl::sql {

JoinHashTableVectorLookup::JoinHashTableVectorLookup(const JoinHashTable &table) noexcept
    : table_(table), match_idx_(0), hashes_{0}, entries_{nullptr} {}

void JoinHashTableVectorLookup::Prepare(ProjectedColumnsIterator *pci, const HashFn hash_fn) noexcept {
  TPL_ASSERT(pci->num_selected() <= kDefaultVectorSize, "VectorProjection size must be less than kDefaultVectorSize");
  // Set up
  match_idx_ = 0;

  // Compute the hashes
  {
    u32 idx = 0;
    pci->ForEach([this, pci, hash_fn, &idx]() { hashes_[idx++] = hash_fn(pci); });
  }

  // Perform the initial lookup
  table_.LookupBatch(pci->num_selected(), hashes_, entries_);
}

}  // namespace tpl::sql
