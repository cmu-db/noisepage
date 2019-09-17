#pragma once

#include "execution/sql/hash_table_entry.h"
#include "execution/sql/projected_columns_iterator.h"
#include "execution/util/execution_common.h"

namespace terrier::execution::sql {

class JoinHashTable;

/**
 * Helper class to perform vectorized lookups into a JoinHashTable
 */
class EXPORT JoinHashTableVectorProbe {
 public:
  /**
   * Function to hash the tuple the iterator is currently pointing at.
   */
  using HashFn = hash_t (*)(ProjectedColumnsIterator *);

  /**
   * Function to check if the tuple in the hash table (i.e., the first argument)
   * is equivalent to the tuple the iterator is currently pointing at.
   */
  using KeyEqFn = bool (*)(const void *, ProjectedColumnsIterator *);

  /**
   * Constructor given a hashing function and a key equality function
   */
  explicit JoinHashTableVectorProbe(const JoinHashTable &table);

  /**
   * Setup a vectorized lookup using the given input batch @em pci
   * @param pci The input vector
   * @param hash_fn The hashing function
   */
  void Prepare(ProjectedColumnsIterator *pci, HashFn hash_fn);

  /**
   * Return the next match, moving the input iterator if need be
   * @param pci The input vector projection
   * @param key_eq_fn The function to check key equality
   * @return The next matching entry
   */
  const HashTableEntry *GetNextOutput(ProjectedColumnsIterator *pci, KeyEqFn key_eq_fn);

 private:
  // The table we're probing
  const JoinHashTable &table_;
  // The current index in the entries output we're iterating over
  uint16_t match_idx_;
  // The vector of computed hashes
  hash_t hashes_[common::Constants::K_DEFAULT_VECTOR_SIZE];
  // The vector of entries
  const HashTableEntry *entries_[common::Constants::K_DEFAULT_VECTOR_SIZE];
};

// ---------------------------------------------------------
// Implementation below
// ---------------------------------------------------------

// Because this function is a tuple-at-a-time, it's placed in the header to
// reduce function call overhead.
inline const HashTableEntry *JoinHashTableVectorProbe::GetNextOutput(ProjectedColumnsIterator *const pci,
                                                                     const KeyEqFn key_eq_fn) {
  TERRIER_ASSERT(pci != nullptr, "No input PCI!");
  TERRIER_ASSERT(match_idx_ < pci->NumSelected(), "Continuing past iteration!");

  while (true) {
    // Continue along current chain until we find a match
    while (const auto *entry = entries_[match_idx_]) {
      entries_[match_idx_] = entry->next_;
      if (entry->hash_ == hashes_[match_idx_] && key_eq_fn(entry->payload_, pci)) {
        return entry;
      }
    }

    // No match found, move to the next probe tuple index
    if (++match_idx_ >= pci->NumSelected()) {
      break;
    }

    // Advance probe input
    if (pci->IsFiltered()) {
      pci->AdvanceFiltered();
    } else {
      pci->Advance();
    }
  }

  return nullptr;
}

}  // namespace terrier::execution::sql
