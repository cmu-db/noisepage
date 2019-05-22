#pragma once

#include "execution/sql/bloom_filter.h"
#include "execution/sql/concise_hash_table.h"
#include "execution/sql/generic_hash_table.h"
#include "execution/util/chunked_vector.h"
#include "execution/util/region.h"

namespace tpl::sql::test {
class JoinHashTableTest;
}  // namespace tpl::sql::test

namespace tpl::sql {

class JoinHashTable {
 public:
  /// Construct a hash-table used for join processing using \a region as the
  /// main memory allocator
  JoinHashTable(util::Region *region, u32 tuple_size, bool use_concise_ht = false) noexcept;

  /// This class cannot be copied or moved
  DISALLOW_COPY_AND_MOVE(JoinHashTable);

  /// Allocate storage in the hash table for an input tuple whose hash value is
  /// \a hash and whose size (in bytes) is \a tuple_size. Remember that this
  /// only performs an allocation from the table's memory pool. No insertion
  /// into the table is performed.
  byte *AllocInputTuple(hash_t hash);

  /// Fully construct the join hash table. If the join hash table has already
  /// been built, do nothing.
  void Build();

  /// The tuple-at-a-time iterator
  class Iterator;

  /// Lookup a single entry with hash value \a hash returning an iterator
  template <bool UseCHT>
  Iterator Lookup(hash_t hash) const;

  /// Perform a vectorized lookup
  void LookupBatch(u32 num_tuples, const hash_t hashes[], const HashTableEntry *results[]) const;

  /// Return the amount of memory the buffered tuples occupy
  u64 GetBufferedTupleMemoryUsage() const noexcept { return entries_.size() * entries_.element_size(); }

  /// Get the amount of memory used by the join index only (i.e., excluding
  /// space used to store materialized build-side tuples)
  u64 GetJoinIndexMemoryUsage() const noexcept {
    return use_concise_hash_table() ? concise_hash_table_.GetTotalMemoryUsage()
                                    : generic_hash_table_.GetTotalMemoryUsage();
  }

  /// Return the total size of the join hash table in bytes
  u64 GetTotalMemoryUsage() const noexcept { return GetBufferedTupleMemoryUsage() + GetJoinIndexMemoryUsage(); }

  // -------------------------------------------------------
  // Simple Accessors
  // -------------------------------------------------------

  /// Return the total number of inserted elements, including duplicates
  u64 num_elements() const noexcept { return entries_.size(); }

  /// Has the hash table been built?
  bool is_built() const noexcept { return built_; }

  /// Is this join using a concise hash table?
  bool use_concise_hash_table() const noexcept { return use_concise_ht_; }

 public:
  // -------------------------------------------------------
  // Tuple-at-a-time Iterator
  // -------------------------------------------------------

  /// The iterator used for generic lookups. This class is used mostly for
  /// tuple-at-a-time lookups from the hash table.
  class Iterator {
   public:
    Iterator(const HashTableEntry *initial, hash_t hash);

    using KeyEq = bool(void *opaque_ctx, void *probe_tuple, void *table_tuple);
    const HashTableEntry *NextMatch(KeyEq key_eq, void *opaque_ctx, void *probe_tuple);

   private:
    // The next element the iterator produces
    const HashTableEntry *next_;
    // The hash value we're looking up
    hash_t hash_;
  };

 private:
  friend class tpl::sql::test::JoinHashTableTest;

  // Access a stored entry by index
  HashTableEntry *EntryAt(const u64 idx) noexcept { return reinterpret_cast<HashTableEntry *>(entries_[idx]); }

  const HashTableEntry *EntryAt(const u64 idx) const noexcept {
    return reinterpret_cast<const HashTableEntry *>(entries_[idx]);
  }

  // Dispatched from Build() to build either a generic or concise hash table
  void BuildGenericHashTable() noexcept;
  void BuildConciseHashTable();

  // Dispatched from BuildGenericHashTable()
  template <bool Prefetch>
  void BuildGenericHashTableInternal() noexcept;

  // Dispatched from BuildConciseHashTable() to construct the concise hash table
  // and to reorder buffered build tuples in place according to the CHT
  template <bool PrefetchCHT, bool PrefetchEntries>
  void BuildConciseHashTableInternal();
  template <bool Prefetch>
  void InsertIntoConciseHashTable() noexcept;
  template <bool PrefetchCHT, bool PrefetchEntries>
  void ReorderMainEntries() noexcept;
  template <bool Prefetch, bool PrefetchEntries>
  void ReorderOverflowEntries() noexcept;
  void VerifyMainEntryOrder();
  void VerifyOverflowEntryOrder() noexcept;

  // Dispatched from LookupBatch() to lookup from either a generic or concise
  // hash table in batched manner
  void LookupBatchInGenericHashTable(u32 num_tuples, const hash_t hashes[], const HashTableEntry *results[]) const;
  void LookupBatchInConciseHashTable(u32 num_tuples, const hash_t hashes[], const HashTableEntry *results[]) const;

  // Dispatched from LookupBatchInGenericHashTable()
  template <bool Prefetch>
  void LookupBatchInGenericHashTableInternal(u32 num_tuples, const hash_t hashes[],
                                             const HashTableEntry *results[]) const;

  // Dispatched from LookupBatchInConciseHashTable()
  template <bool Prefetch>
  void LookupBatchInConciseHashTableInternal(u32 num_tuples, const hash_t hashes[],
                                             const HashTableEntry *results[]) const;

 private:
  // The vector where we store the build-side input
  util::ChunkedVector entries_;

  // The generic hash table
  GenericHashTable generic_hash_table_;

  // The concise hash table
  ConciseHashTable concise_hash_table_;

  // The bloom filter
  BloomFilter bloom_filter_;

  // Has the hash table been built?
  bool built_;

  // Should we use a concise hash table?
  bool use_concise_ht_;
};

// ---------------------------------------------------------
// JoinHashTable implementation
// ---------------------------------------------------------

template <>
inline JoinHashTable::Iterator JoinHashTable::Lookup<false>(const hash_t hash) const {
  HashTableEntry *entry = generic_hash_table_.FindChainHead(hash);
  while (entry != nullptr && entry->hash != hash) {
    entry = entry->next;
  }
  return JoinHashTable::Iterator(entry, hash);
}

template <>
inline JoinHashTable::Iterator JoinHashTable::Lookup<true>(const hash_t hash) const {
  const auto lookup_res = concise_hash_table_.Lookup(hash);
  auto found = lookup_res.first;
  auto idx = lookup_res.second;
  auto *entry = (found ? EntryAt(idx) : nullptr);
  return JoinHashTable::Iterator(entry, hash);
}

// ---------------------------------------------------------
// JoinHashTable's Iterator implementation
// ---------------------------------------------------------

inline JoinHashTable::Iterator::Iterator(const HashTableEntry *initial, hash_t hash) : next_(initial), hash_(hash) {}

inline const HashTableEntry *JoinHashTable::Iterator::NextMatch(JoinHashTable::Iterator::KeyEq key_eq, void *opaque_ctx,
                                                                void *probe_tuple) {
  const HashTableEntry *result = next_;
  while (result != nullptr) {
    next_ = next_->next;
    if (result->hash == hash_ &&
        key_eq(opaque_ctx, probe_tuple, reinterpret_cast<void *>(const_cast<byte *>(result->payload)))) {
      break;
    }
    result = next_;
  }
  return result;
}

}  // namespace tpl::sql
