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

/**
 * Hash Table used for joins.
 */
class JoinHashTable {
 public:
  /**
   * Construct a hash-table used for join processing using a region as the main memory allocator
   * @param region region to use for allocation
   * @param tuple_size size of the tuples
   * @param use_concise_ht whether to use a concise implementation or not
   */
  JoinHashTable(util::Region *region, u32 tuple_size, bool use_concise_ht = false) noexcept;

  /**
   * This class cannot be copied or moved
   */
  DISALLOW_COPY_AND_MOVE(JoinHashTable);

  /**
  * Allocate storage in the hash table for an input tuple whose hash value is
  * hash and whose size (in bytes) is tuple_size. Remember that this
  * only performs an allocation from the table's memory pool. No insertion
  * into the table is performed.
   *
   * @param hash hash value of the inserted tuple
   * @return byte array where the tuple can be written into
   */
  byte *AllocInputTuple(hash_t hash);

  /**
   * Fully construct the join hash table. If the join hash table has already been built, do nothing.
   */
  void Build();

  /**
   * The tuple-at-a-time iterator
   */
  class Iterator;

  /**
   * Lookup a single entry with the given hash value returning an iterator
   * @tparam UseCHT whether to use a concise implementation or not
   * @param hash hash value to lookup
   * @return iterator over the matches
   */
  template <bool UseCHT>
  Iterator Lookup(hash_t hash) const;

  /**
   * Perform a vectorized lookup
   * @param num_tuples batch size
   * @param hashes hashes of the elements to lookup
   * @param results matches found
   */
  void LookupBatch(u32 num_tuples, const hash_t hashes[], const HashTableEntry *results[]) const;

  /**
   * @return the amount of memory the buffered tuples occupy
   */
  u64 GetBufferedTupleMemoryUsage() const noexcept { return entries_.size() * entries_.element_size(); }

  /**
   * @return Get the amount of memory used by the join index only (i.e., excluding space used to store materialized build-side tuples)
   */
  u64 GetJoinIndexMemoryUsage() const noexcept {
    return use_concise_hash_table() ? concise_hash_table_.GetTotalMemoryUsage()
                                    : generic_hash_table_.GetTotalMemoryUsage();
  }

  /**
   * @return the total size of the join hash table in bytes
   */
  u64 GetTotalMemoryUsage() const noexcept { return GetBufferedTupleMemoryUsage() + GetJoinIndexMemoryUsage(); }

  // -------------------------------------------------------
  // Simple Accessors
  // -------------------------------------------------------

  /**
   * @return the total number of inserted elements, including duplicates
   */
  u64 num_elements() const noexcept { return entries_.size(); }

  /**
   * @return Has the hash table been built?
   */
  bool is_built() const noexcept { return built_; }

  /**
   * @return Is this join using a concise hash table?
   */
  bool use_concise_hash_table() const noexcept { return use_concise_ht_; }

 public:
  // -------------------------------------------------------
  // Tuple-at-a-time Iterator
  // -------------------------------------------------------

  /**
   * The iterator used for generic lookups. This class is used mostly for tuple-at-a-time lookups from the hash table.
   */
  class Iterator {
   public:
    /**
     * Constructor of the iterator
     * @param initial first entry in the iterator
     * @param hash hash value of the matching tuple
     */
    Iterator(const HashTableEntry *initial, hash_t hash);

    /**
     * Equality function
     */
    using KeyEq = bool(void *opaque_ctx, void *probe_tuple, void *table_tuple);

    /**
     * Return the next match of the given tuple.
     * @param key_eq equality function to use
     * @param opaque_ctx helper context used by the equality function
     * @param probe_tuple the probe tuple
     * @return the next match of the given tuple.
     */
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

/**
 * Lookup for non-concise hash table
 * @param hash hash value to lookup
 * @return iterator for matches found
 */
template <>
inline JoinHashTable::Iterator JoinHashTable::Lookup<false>(const hash_t hash) const {
  HashTableEntry *entry = generic_hash_table_.FindChainHead(hash);
  while (entry != nullptr && entry->hash != hash) {
    entry = entry->next;
  }
  return JoinHashTable::Iterator(entry, hash);
}

/**
 * Lookup for concise hash table
 * @param hash hash value to lookup
 * @return iterator for matches found
 */
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
