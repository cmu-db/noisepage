#include "sql/chaining_hash_table.h"

#include <algorithm>
#include <limits>

#include "util/math_util.h"

namespace tpl::sql {

//===----------------------------------------------------------------------===//
//
// Chaining Hash Table Base
//
//===----------------------------------------------------------------------===//

ChainingHashTableBase::ChainingHashTableBase(float load_factor) noexcept
    : entries_(nullptr), mask_(0), capacity_(0), load_factor_(load_factor) {}

ChainingHashTableBase::~ChainingHashTableBase() {
  if (entries_ != nullptr) {
    Memory::FreeHugeArray(entries_, GetCapacity());
  }
}

void ChainingHashTableBase::SetSize(uint64_t new_size) {
  new_size = std::max(new_size, kMinTableSize);

  if (entries_ != nullptr) {
    Memory::FreeHugeArray(entries_, GetCapacity());
  }

  uint64_t next_size = util::MathUtil::PowerOf2Ceil(new_size);
  if (next_size < new_size / load_factor_) {
    next_size *= 2;
  }

  capacity_ = next_size;
  mask_ = capacity_ - 1;
  entries_ = Memory::MallocHugeArray<HashTableEntry *>(capacity_, true);
}

//===----------------------------------------------------------------------===//
//
// Chaining Hash Table
//
//===----------------------------------------------------------------------===//

template <bool UseTags>
ChainingHashTable<UseTags>::ChainingHashTable(float load_factor)
    : ChainingHashTableBase(load_factor), num_elements_(0) {}

template <bool UseTags>
void ChainingHashTable<UseTags>::SetSize(uint64_t new_size) {
  num_elements_ = 0;
  ChainingHashTableBase::SetSize(new_size);
}

template <bool UseTags>
std::tuple<uint64_t, uint64_t, float> ChainingHashTable<UseTags>::GetChainLengthStats() const {
  uint64_t min = std::numeric_limits<uint64_t>::max(), max = 0, total = 0;

  for (uint64_t idx = 0; idx < capacity_; idx++) {
    HashTableEntry *entry = entries_[idx];
    if constexpr (UseTags) {
      entry = UntagPointer(entry);
    }
    uint64_t length = 0;
    for (; entry != nullptr; entry = entry->next) {
      total++;
      length++;
    }
    min = std::min(min, length);
    max = std::max(max, length);
  }

  return {min, max, static_cast<float>(total) / capacity_};
}

template class ChainingHashTable<true>;
template class ChainingHashTable<false>;

//===----------------------------------------------------------------------===//
//
// Vector Iterator
//
//===----------------------------------------------------------------------===//

template <bool UseTag>
ChainingHashTableVectorIterator<UseTag>::ChainingHashTableVectorIterator(
    const ChainingHashTable<UseTag> &table, MemoryPool *memory) noexcept
    : memory_(memory),
      table_(table),
      table_dir_index_(0),
      entry_vec_(
          memory_->AllocateArray<const HashTableEntry *>(kDefaultVectorSize, CACHELINE_SIZE, true)),
      entry_vec_end_idx_(0) {
  Next();
}

template <bool UseTag>
ChainingHashTableVectorIterator<UseTag>::~ChainingHashTableVectorIterator() {
  memory_->DeallocateArray(entry_vec_, kDefaultVectorSize);
}

template <bool UseTag>
void ChainingHashTableVectorIterator<UseTag>::Next() {
  // Invariant: the range of elements [0, entry_vec_end_idx_) in the entry cache
  // contains non-null hash table entries.

  // Index tracks the end of the valid range of entries in the entry cache
  uint32_t index = 0;

  // For the current set of valid entries, follow their chain. This may produce
  // holes in the range, but we'll compact them out in a subsequent filter.
  for (uint32_t i = 0, prefetch_idx = kPrefetchDistance; i < entry_vec_end_idx_; i++) {
    if (TPL_LIKELY(prefetch_idx < entry_vec_end_idx_)) {
      Memory::Prefetch<true, Locality::Low>(entry_vec_[prefetch_idx++]);
    }
    entry_vec_[i] = entry_vec_[i]->next;
  }

  // Compact out the holes produced in the previous chain lookup.
  for (uint32_t i = 0; i < entry_vec_end_idx_; i++) {
    entry_vec_[index] = entry_vec_[i];
    index += (entry_vec_[index] != nullptr);
  }

  // Fill the range [idx, SIZE) in the cache with valid entries from the source
  // hash table.
  while (index < kDefaultVectorSize && table_dir_index_ < table_.GetCapacity()) {
    entry_vec_[index] = table_.entries_[table_dir_index_++];
    if constexpr (UseTag) {
      entry_vec_[index] = ChainingHashTable<UseTag>::UntagPointer(entry_vec_[index]);
    }
    index += (entry_vec_[index] != nullptr);
  }

  // The new range of valid entries is in [0, idx).
  entry_vec_end_idx_ = index;
}

template class ChainingHashTableVectorIterator<true>;
template class ChainingHashTableVectorIterator<false>;

}  // namespace tpl::sql
