#pragma once

#include <memory>
#include <vector>

#include "common/macros.h"
#include "common/managed_pointer.h"
#include "common/spin_latch.h"
#include "execution/sql/bloom_filter.h"
#include "execution/sql/chaining_hash_table.h"
#include "execution/sql/concise_hash_table.h"
#include "execution/sql/memory_pool.h"
#include "execution/util/chunked_vector.h"

namespace libcount {
class HLL;
}  // namespace libcount

namespace terrier::execution::exec {
class ExecutionSettings;
class ExecutionContext;
}  // namespace terrier::execution::exec

namespace terrier::execution::sql {

class ThreadStateContainer;
class Vector;

/**
 * The main class used to for hash joins. JoinHashTables are bulk-loaded through calls to
 * JoinHashTable::AllocInputTuple() and lazily built through JoinHashTable::Build(). After a
 * JoinHashTable has been built once, it is frozen and immutable. Thus, they're write-once read-many
 * (WORM) structures.
 *
 * @code
 * JoinHashTable jht = ...
 * for (tuple in table) {
 *   auto tuple = reinterpret_cast<YourTuple *>(jht.AllocInputTuple());
 *   tuple->col_a = ...
 *   ...
 * }
 * // All insertions complete, lazily build the table
 * jht.Build();
 * @endcode
 *
 * In parallel mode, thread-local join hash tables are lazily built and merged in parallel into a
 * global join hash table through a call to JoinHashTable::MergeParallel(). After this call, the
 * global table takes ownership of all thread-local allocated memory and hash index.
 */
class EXPORT JoinHashTable {
 public:
  /** Used to denote the offsets into ExecutionContext::hooks_ of particular functions */
  enum class HookOffsets : uint32_t {
    StartHook = 0,
    EndHook,

    NUM_HOOKS
  };

  /** Default precision to use for HLL estimations. */
  static constexpr uint32_t DEFAULT_HLL_PRECISION = 10;

  /** Minimum number of expected elements to merge before triggering a parallel merge. */
  static constexpr uint32_t DEFAULT_MIN_SIZE_FOR_PARALLEL_MERGE = 1024;

  /**
   * Construct a join hash table. All memory allocations are sourced from the injected @em memory,
   * and thus, are ephemeral.
   * @param exec_settings The execution settings to use.
   * @param exec_ctx ExecutionContext
   * @param tuple_size The size of the tuple stored in this join hash table.
   * @param use_concise_ht Whether to use a concise or fatter chaining join index.
   */
  explicit JoinHashTable(const exec::ExecutionSettings &exec_settings, exec::ExecutionContext *exec_ctx,
                         uint32_t tuple_size, bool use_concise_ht = false);

  /**
   * This class cannot be copied or moved.
   */
  DISALLOW_COPY_AND_MOVE(JoinHashTable);

  /**
   * Destructor.
   */
  ~JoinHashTable();

  /**
   * Allocate storage in the hash table for an input tuple whose hash value is @em hash. This
   * function only performs an allocation from the table's memory pool. No insertion into the table
   * is performed, meaning a subsequent JoinHashTable::Lookup() for the entry will not return the
   * inserted entry.
   * @param hash The hash value of the tuple to insert.
   * @return A memory region where the caller can materialize the tuple.
   */
  byte *AllocInputTuple(hash_t hash);

  /**
   * Build and finalize the join hash table. After finalization, no new insertions are allowed and
   * the table becomes read-only. Nothing is done if the join hash table has already been finalized.
   */
  void Build();

  /**
   * Lookup a single entry with hash value @em hash returning an iterator.
   * @tparam UseCHT Should the lookup use the concise or general table.
   * @param hash The hash value of the element to lookup.
   * @return An iterator over all elements that match the hash.
   */
  template <bool UseCHT>
  HashTableEntryIterator Lookup(hash_t hash) const;

  /**
   * Perform a bulk lookup of tuples whose hash values are stored in @em hashes, storing the results
   * in @em results. The results vector will chain the potentially null head of a chain of
   * HashTableEntry objects.
   * @param hashes The hash values of the probe elements.
   * @param results The heads of the bucket chain of the probed elements.
   */
  void LookupBatch(const Vector &hashes, Vector *results) const;

  /**
   * Merge all thread-local hash tables stored in the state contained into this table. Perform the
   * merge in parallel.
   * @param thread_state_container The container for all thread-local tables.
   * @param jht_offset The offset in the state where the hash table is.
   */
  void MergeParallel(ThreadStateContainer *thread_state_container, std::size_t jht_offset);

  /**
   * @return The total number of bytes used to materialize tuples. This excludes space required for
   *         the join index.
   */
  uint64_t GetBufferedTupleMemoryUsage() const { return entries_.size() * entries_.ElementSize(); }

  /**
   * @return The total number of bytes used by the join index only. The join index (also referred to
   *         as the hash table directory), excludes storage for materialized tuple contents.
   */
  uint64_t GetJoinIndexMemoryUsage() const {
    return UsingConciseHashTable() ? concise_hash_table_.GetTotalMemoryUsage()
                                   : chaining_hash_table_.GetTotalMemoryUsage();
  }

  /**
   * @return The total number of bytes used by this table. This includes both the raw tuple storage
   *         and the hash table directory storage.
   */
  uint64_t GetTotalMemoryUsage() const { return GetBufferedTupleMemoryUsage() + GetJoinIndexMemoryUsage(); }

  /** @return The execution settings in use for this JoinHashTable. */
  const exec::ExecutionSettings &GetExecutionSettings() const { return exec_settings_; }

  /**
   * @return True if this table uses an early filtering bloom filter; false otherwise.
   */
  bool HasBloomFilter() const { return !bloom_filter_.IsEmpty(); }

  /**
   * @return The total number of elements in the table, including duplicates.
   */
  uint64_t GetTupleCount() const {
    // We don't know if this hash table was built in parallel. To be safe, we
    // acquire the lock before checking the owned entries vector. This isn't a
    // performance critical function, so locking should be okay ...
    common::SpinLatch::ScopedSpinLatch latch(&owned_latch_);
    if (!owned_.empty()) {
      uint64_t count = 0;
      for (const auto &entries : owned_) {
        count += entries.size();
      }
      return count;
    }

    return entries_.size();
  }

  /**
   * @return True if the join hash table has been built; false otherwise.
   */
  bool IsBuilt() const { return built_; }

  /**
   * @return True if this join hash table uses a concise table under the hood.
   */
  bool UsingConciseHashTable() const { return use_concise_ht_; }

  /**
   * @return The underlying bloom filter.
   */
  const BloomFilter *GetBloomFilter() const { return &bloom_filter_; }

 private:
  friend class JoinHashTableIterator;
  FRIEND_TEST(JoinHashTableTest, LazyInsertionTest);
  FRIEND_TEST(JoinHashTableTest, PerfTest);

  // Access a stored entry by index
  HashTableEntry *EntryAt(const uint64_t idx) { return reinterpret_cast<HashTableEntry *>(entries_[idx]); }

  const HashTableEntry *EntryAt(const uint64_t idx) const {
    return reinterpret_cast<const HashTableEntry *>(entries_[idx]);
  }

  // Dispatched from Build() to build either a chaining or concise hash table.
  void BuildChainingHashTable();
  void BuildConciseHashTable();

  // Dispatched from BuildConciseHashTable() to construct the concise hash table
  // and to reorder buffered build tuples in place according to the CHT.
  template <bool PrefetchCHT, bool PrefetchEntries>
  void BuildConciseHashTableInternal();
  template <bool PrefetchCHT, bool PrefetchEntries>
  void ReorderMainEntries();
  template <bool Prefetch, bool PrefetchEntries>
  void ReorderOverflowEntries();
  void VerifyMainEntryOrder();
  void VerifyOverflowEntryOrder();

  // Dispatched from LookupBatch() to lookup from either a chaining or concise
  // hash table in batched manner.
  void LookupBatchInChainingHashTable(const Vector &hashes, Vector *results) const;
  void LookupBatchInConciseHashTable(const Vector &hashes, Vector *results) const;

  // Merge the source hash table (which isn't built yet) into this one
  template <bool Concurrent>
  void MergeIncomplete(JoinHashTable *source);

 private:
  // The execution context to run with.
  const exec::ExecutionSettings &exec_settings_;

  exec::ExecutionContext *exec_ctx_;

  // The vector where we store the build-side input.
  util::ChunkedVector<MemoryPoolAllocator<byte>> entries_;

  // To protect concurrent access to 'owned_entries_'.
  mutable common::SpinLatch owned_latch_;

  // List of entries this hash table has taken ownership of.
  // Protected by 'owned_latch_'.
  MemPoolVector<decltype(entries_)> owned_;

  // The chaining hash table.
  TaggedChainingHashTable chaining_hash_table_;

  // The concise hash table.
  ConciseHashTable concise_hash_table_;

  // The bloom filter.
  BloomFilter bloom_filter_;

  // Estimator of unique elements.
  std::unique_ptr<libcount::HLL> hll_estimator_;

  // Has the hash table been built?
  bool built_;

  // Should we use a concise hash table?
  bool use_concise_ht_;

  // MemoryTracker
  common::ManagedPointer<MemoryTracker> tracker_;
};

// ---------------------------------------------------------
// JoinHashTable implementation
// ---------------------------------------------------------

/** Look up the specified hash, do not use the concise hash table. */
template <>
inline HashTableEntryIterator JoinHashTable::Lookup<false>(const hash_t hash) const {
  HashTableEntry *entry = chaining_hash_table_.FindChainHead(hash);
  while (entry != nullptr && entry->hash_ != hash) {
    entry = entry->next_;
  }
  return HashTableEntryIterator(entry, hash);
}

/** Look up the specified hash, use the concise hash table. */
template <>
inline HashTableEntryIterator JoinHashTable::Lookup<true>(const hash_t hash) const {
  const auto [found, idx] = concise_hash_table_.Lookup(hash);
  auto *entry = (found ? EntryAt(idx) : nullptr);
  return HashTableEntryIterator(entry, hash);
}

//===----------------------------------------------------------------------===//
//
// Join Hash Table Iterator
//
//===----------------------------------------------------------------------===//
/**
 * There are two distinct iterators related to hash tables. The HashTableEntryIterator
 * defined in hash_table_entry.h takes in a hash value and iterates over all entries in
 * the hash table whose hash matches the given hash. The iterator defined below simply
 * iterates over all the entries of the join hash table one tuple at a time.
 *
 * The join hash table must be fully built either through a serial call to JoinHashTable::Build()
 * or merged (in parallel) from other join hash tables through JoinHashTable::MergeParallel().
 *
 * Users use the OOP-ish iteration API:
 * for (JoinHashTableIterator iter(table); iter.HasNext(); iter.Next()) {
 *   auto row = iter.GetCurrentRowAs<MyFancyAssType>();
 *   ...
 * }
 *
 * // TODO(pmenon): Vectorized version, too.
 */
class JoinHashTableIterator {
 public:
  /**
   * Construct an iterator over the given hash table.
   * @param table The join hash table to iterate.
   */
  explicit JoinHashTableIterator(const JoinHashTable &table);
  /**
   * @return True if there is more data in the iterator; false otherwise.
   */
  bool HasNext() const noexcept { return entry_iter_ != entry_end_; }
  /**
   * Advance to the next tuple.
   */
  void Next() noexcept {
    // Advance the entry iterator by one.
    ++entry_iter_;
    // If we've exhausted the current entry list, find another.
    if (entry_iter_ == entry_end_) {
      FindNextNonEmptyList();
    }
  }
  /**
   * Access the row the iterator is currently positioned at.
   * @pre A previous call to HasNext() must have returned true.
   * @return A read-only opaque byte pointer to the row at the current iteration position.
   */
  const byte *GetCurrentRow() const noexcept {
    TERRIER_ASSERT(HasNext(), "HasNext() indicates no more data!");
    const auto entry = reinterpret_cast<const HashTableEntry *>(*entry_iter_);
    return entry->payload_;
  }

  /**
   * Access the row the iterator is currently positioned at as the given template type.
   * @pre A previous call to HasNext() must have returned true.
   * @tparam T The type of the row.
   * @return A typed read-only pointer to row at the current iteration position.
   */
  template <typename T>
  const T *GetCurrentRowAs() const noexcept {
    return reinterpret_cast<const T *>(GetCurrentRow());
  }

 private:
  // Advance past any empty entry lists.
  void FindNextNonEmptyList();

 private:
  using EntryListIterator = decltype(JoinHashTable::owned_)::const_iterator;
  using EntryIterator = decltype(JoinHashTable::entries_)::Iterator;
  // An iterator over the entry lists owned by the join hash table.
  EntryListIterator entry_list_iter_, entry_list_end_;
  // An iterator over the entries in a single entry list.
  EntryIterator entry_iter_, entry_end_;
};

}  // namespace terrier::execution::sql
