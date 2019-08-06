#pragma once

#include <memory>
#include <vector>

#include "execution/sql/bloom_filter.h"
#include "execution/sql/concise_hash_table.h"
#include "execution/sql/generic_hash_table.h"
#include "execution/sql/memory_pool.h"
#include "execution/util/chunked_vector.h"
#include "execution/util/spin_latch.h"

namespace libcount {
class HLL;
}  // namespace libcount

namespace terrier::sql::test {
class JoinHashTableTest;
}  // namespace terrier::sql::test

namespace terrier::sql {

class ThreadStateContainer;
class JoinHashTableIterator;

/**
 * The main join hash table. Join hash tables are bulk-loaded through calls to
 * @em AllocInputTuple() and frozen after calling @em Build(). Thus, they're
 * write-once read-many (WORM) structures.
 */
class JoinHashTable {
 public:
  /**
   * Default HLL precision
   */
  static constexpr u32 kDefaultHLLPrecision = 10;

  /**
   * Construct a join hash table. All memory allocations are sourced from the
   * injected @em memory, and thus, are ephemeral.
   * @param memory The memory pool to allocate memory from
   * @param tuple_size The size of the tuple stored in this join hash table
   * @param use_concise_ht Whether to use a concise or generic join index
   */
  explicit JoinHashTable(MemoryPool *memory, u32 tuple_size, bool use_concise_ht = false);

  /**
   * This class cannot be copied or moved
   */
  DISALLOW_COPY_AND_MOVE(JoinHashTable);

  /**
   * Destructor
   */
  ~JoinHashTable();

  /**
   * Allocate storage in the hash table for an input tuple whose hash value is
   * @em hash. This function only performs an allocation from the table's memory
   * pool. No insertion into the table is performed, meaning a subsequent
   * @em Lookup() for the entry will not return the inserted entry.
   * @param hash The hash value of the tuple to insert
   * @return A memory region where the caller can materialize the tuple
   */
  byte *AllocInputTuple(hash_t hash);

  /**
   * Fully construct the join hash table. Nothing is done if the join hash table
   * has already been built. After building, the table becomes read-only.
   */
  void Build();

  /**
   * Lookup a single entry with hash value @em hash returning an iterator
   * @tparam UseCHT Should the lookup use the concise or general table
   * @param hash The hash value of the element to lookup
   * @return An iterator over all elements that match the hash
   */
  template <bool UseCHT>
  JoinHashTableIterator Lookup(hash_t hash) const;

  /**
   * Perform a batch lookup of elements whose hash values are stored in @em
   * hashes, storing the results in @em results
   * @param num_tuples The number of tuples in the batch
   * @param hashes The hash values of the probe elements
   * @param results The heads of the bucket chain of the probed elements
   */
  void LookupBatch(u32 num_tuples, const hash_t hashes[], const HashTableEntry *results[]) const;

  /**
   * Merge all thread-local hash tables stored in the state contained into this
   * table. Perform the merge in parallel.
   * @param thread_state_container The container for all thread-local tables
   * @param jht_offset The offset in the state where the hash table is
   */
  void MergeParallel(const ThreadStateContainer *thread_state_container, u32 jht_offset);

  // -------------------------------------------------------
  // Accessors
  // -------------------------------------------------------

  /**
   * Return the amount of memory the buffered tuples occupy
   */
  u64 GetBufferedTupleMemoryUsage() const noexcept { return entries_.size() * entries_.element_size(); }

  /**
   * Get the amount of memory used by the join index only (i.e., excluding space
   * used to store materialized build-side tuples)
   */
  u64 GetJoinIndexMemoryUsage() const noexcept {
    return use_concise_hash_table() ? concise_hash_table_.GetTotalMemoryUsage()
                                    : generic_hash_table_.GetTotalMemoryUsage();
  }

  /**
   * Return the total size of the join hash table in bytes
   */
  u64 GetTotalMemoryUsage() const noexcept { return GetBufferedTupleMemoryUsage() + GetJoinIndexMemoryUsage(); }

  /**
   * Return the total number of inserted elements, including duplicates
   */
  u64 num_elements() const noexcept { return entries_.size(); }

  /**
   * Has the hash table been built?
   */
  bool is_built() const noexcept { return built_; }

  /**
   * Is this join using a concise hash table?
   */
  bool use_concise_hash_table() const noexcept { return use_concise_ht_; }

 private:
  friend class terrier::sql::test::JoinHashTableTest;

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

  // Merge the source hash table (which isn't built yet) into this one
  template <bool Prefetch, bool Concurrent>
  void MergeIncomplete(JoinHashTable *source);

 private:
  // The vector where we store the build-side input
  util::ChunkedVector<MemoryPoolAllocator<byte>> entries_;

  // To protect concurrent access to owned_entries
  util::SpinLatch owned_latch_;
  // List of entries this hash table has taken ownership of
  MemPoolVector<decltype(entries_)> owned_;

  // The generic hash table
  GenericHashTable generic_hash_table_;

  // The concise hash table
  ConciseHashTable concise_hash_table_;

  // The bloom filter
  BloomFilter bloom_filter_;

  // Estimator of unique elements
  std::unique_ptr<libcount::HLL> hll_estimator_;

  // Has the hash table been built?
  bool built_;

  // Should we use a concise hash table?
  bool use_concise_ht_;
};

/**
 * The iterator used for generic lookups. This class is used mostly for
 * tuple-at-a-time lookups from the hash table.
 */
class JoinHashTableIterator {
 public:
  /**
   * Construct an iterator beginning at the entry @em initial of the chain
   * of entries matching the hash value @em hash. This iterator is returned
   * from @em JoinHashTable::Lookup().
   * @param initial The first matching entry in the chain of entries
   * @param hash The hash value of the probe tuple
   */
  JoinHashTableIterator(const HashTableEntry *initial, hash_t hash);

  /**
   * Function used to check equality of hash keys
   */
  using KeyEq = bool (*)(void *opaque_ctx, void *probe_tuple, void *table_tuple);

  /**
   * Advance to the next match and return true if it is found.
   * @param key_eq The function used to determine key equality
   * @param opaque_ctx An opaque context passed into the key equality function
   * @param probe_tuple The probe tuple
   * @return true iff there is a next match.
   */
  bool HasNext(KeyEq key_eq, void *opaque_ctx, void *probe_tuple);

  /**
   * Return the next match.
   */
  const HashTableEntry *NextMatch();

 private:
  // The next element the iterator produces
  const HashTableEntry *next_;
  // The hash value we're looking up
  hash_t hash_;
};

// ---------------------------------------------------------
// JoinHashTable implementation
// ---------------------------------------------------------

/**
 * Lookup for non-concise implementations
 */
template <>
inline JoinHashTableIterator JoinHashTable::Lookup<false>(const hash_t hash) const {
  HashTableEntry *entry = generic_hash_table_.FindChainHead(hash);
  while (entry != nullptr && entry->hash != hash) {
    entry = entry->next;
  }
  return JoinHashTableIterator(entry, hash);
}

/**
 * Lookup for concise implementations
 */
template <>
inline JoinHashTableIterator JoinHashTable::Lookup<true>(const hash_t hash) const {
  // NOLINTNEXTLINE
  const auto [found, idx] = concise_hash_table_.Lookup(hash);
  auto *entry = (found ? EntryAt(idx) : nullptr);
  return JoinHashTableIterator(entry, hash);
}

// ---------------------------------------------------------
// JoinHashTable's Iterator implementation
// ---------------------------------------------------------

inline JoinHashTableIterator::JoinHashTableIterator(const HashTableEntry *initial, hash_t hash)
    : next_(initial), hash_(hash) {}

inline const HashTableEntry *JoinHashTableIterator::NextMatch() {
  auto result = next_;
  next_ = next_->next;
  return result;
}

inline bool JoinHashTableIterator::HasNext(JoinHashTableIterator::KeyEq key_eq, void *opaque_ctx, void *probe_tuple) {
  while (next_ != nullptr) {
    if (next_->hash == hash_ &&
        key_eq(opaque_ctx, probe_tuple, reinterpret_cast<void *>(const_cast<byte *>(next_->payload)))) {
      return true;
    }
    next_ = next_->next;
  }
  return false;
}

}  // namespace terrier::sql
