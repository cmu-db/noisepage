#include "execution/sql/join_hash_table_vector_probe.h"
#include "execution/sql/projected_columns_iterator.h"

#include "execution/sql/join_hash_table.h"

namespace terrier::execution::sql {

JoinHashTableVectorProbe::JoinHashTableVectorProbe(const JoinHashTable &table)
    : table_(table), match_idx_(0), hashes_{0}, entries_{nullptr} {}

void JoinHashTableVectorProbe::Prepare(ProjectedColumnsIterator *pci, const HashFn hash_fn) {
  TPL_ASSERT(pci->num_selected() <= kDefaultVectorSize, "ProjectedColumns size must be less than kDefaultVectorSize");
  // Set up
  match_idx_ = 0;

  // Compute the hashes
  if (pci->IsFiltered()) {
    for (u32 idx = 0; pci->HasNextFiltered(); pci->AdvanceFiltered()) {
      hashes_[idx++] = hash_fn(pci);
    }
  } else {
    for (u32 idx = 0; pci->HasNext(); pci->Advance()) {
      hashes_[idx++] = hash_fn(pci);
    }
  }

  // Reset the iterator since we just exhausted it from the previous hash
  // computation loop.
  pci->Reset();

  // Perform the initial lookup
  table_.LookupBatch(pci->num_selected(), hashes_, entries_);
}

}  // namespace terrier::execution::sql
