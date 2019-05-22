#pragma once

#include "execution/sql/hash_table_entry.h"
#include "execution/sql/projected_columns_iterator.h"
#include "execution/util/common.h"

namespace tpl::sql {

class JoinHashTable;

/// Helper class to perform vectorized lookups into a JoinHashTable
class JoinHashTableVectorLookup {
 public:
  // clang-format off
  using HashFn = hash_t (*)(ProjectedColumnsIterator *) noexcept;  // NOLINT it appears to parse the function as a cast
  using KeyEqFn = bool (*)(const byte *, ProjectedColumnsIterator *) noexcept;
  // clang-format on

  /// Constructor given a hashing function and a key equality function
  explicit JoinHashTableVectorLookup(const JoinHashTable &table) noexcept;

  /// Setup a vectorized lookup using the given input batch \a pci
  void Prepare(ProjectedColumnsIterator *pci, HashFn hash_fn) noexcept;

  /// Return the next match, moving the input iterator if need be
  const HashTableEntry *GetNextOutput(ProjectedColumnsIterator *pci, KeyEqFn key_eq_fn) noexcept;

 private:
  const JoinHashTable &table_;
  u16 match_idx_;
  hash_t hashes_[kDefaultVectorSize];
  const HashTableEntry *entries_[kDefaultVectorSize];
};

// ---------------------------------------------------------
// Implementation below
// ---------------------------------------------------------

// Because this function is a tuple-at-a-time, it's placed in the header to
// reduce function call overhead.
inline const HashTableEntry *JoinHashTableVectorLookup::GetNextOutput(ProjectedColumnsIterator *pci,
                                                                      const KeyEqFn key_eq_fn) noexcept {
  TPL_ASSERT(pci != nullptr, "No input PCI!");
  TPL_ASSERT(match_idx_ < pci->num_selected(), "Continuing past iteration!");

  while (true) {
    // Continue along current chain until we find a match
    while (const auto *entry = entries_[match_idx_]) {
      entries_[match_idx_] = entry->next;
      if (entry->hash == hashes_[match_idx_] && key_eq_fn(entry->payload, pci)) {
        return entry;
      }
    }

    // No match found, move to the next probe tuple index
    if (++match_idx_ >= pci->num_selected()) {
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

}  // namespace tpl::sql
