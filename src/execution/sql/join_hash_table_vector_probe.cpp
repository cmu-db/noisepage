#include "execution/sql/join_hash_table_vector_probe.h"
#include "execution/sql/projected_columns_iterator.h"

#include "execution/sql/join_hash_table.h"

namespace terrier::execution::sql {

JoinHashTableVectorProbe::JoinHashTableVectorProbe(const JoinHashTable &table)
    : table_(table), match_idx_(0), hashes_{0}, entries_{nullptr} {}

void JoinHashTableVectorProbe::Prepare(ProjectedColumnsIterator *pci, const HashFn hash_fn) {
  TERRIER_ASSERT(pci->NumSelected() <= common::Constants::K_DEFAULT_VECTOR_SIZE,
                 "ProjectedColumns size must be less than common::Constants::K_DEFAULT_VECTOR_SIZE");
  // Set up
  match_idx_ = 0;

  // Compute the hashes
  if (pci->IsFiltered()) {
    for (uint32_t idx = 0; pci->HasNextFiltered(); pci->AdvanceFiltered()) {
      hashes_[idx++] = hash_fn(pci);
    }
  } else {
    for (uint32_t idx = 0; pci->HasNext(); pci->Advance()) {
      hashes_[idx++] = hash_fn(pci);
    }
  }

  // Reset the iterator since we just exhausted it from the previous hash
  // computation loop.
  pci->Reset();

  // Perform the initial lookup
  table_.LookupBatch(pci->NumSelected(), hashes_, entries_);
}

}  // namespace terrier::execution::sql
