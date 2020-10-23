#pragma once

#include <atomic>
#include <tuple>
#include <utility>

#include "common/macros.h"
#include "execution/sql/hash_table_entry.h"
#include "execution/sql/memory_pool.h"
#include "execution/util/chunked_vector.h"
#include "execution/util/cpu_info.h"
#include "execution/util/memory.h"

namespace noisepage::execution::sql {

//===----------------------------------------------------------------------===//
//
// Chaining Hash Table Base
//
//===----------------------------------------------------------------------===//

/**
 * ChainingHashTableBase is the base class implementing a simple bucket-chained hash table with
 * optional pointer tagging. Pointer tagging uses the first @em ChainingHashTableBase::kNumTagBits
 * bits of the entry pointers in the main bucket directory as a tiny bloom filter. This bloom filter
 * is used to early-prune probe misses that would normally require a full cache-unfriendly linked
 * list traversal. We can re-purpose the most-significant 16-bits of a pointer because X86_64 uses
 * 48-bits of the OS virtual memory address space. Pointer tagged hash tables will have to be
 * disabled when the full 64-bit VM address space is enabled.
 *
 * The use of pointer tagging is controlled through proper API usage: InsertTagged() performs a
 * tagged insertion, while InsertUntagged() performs an untagged insertion. This dichotomy carries
 * over when probing the hash table: FindChainHeadTagged() and FindChainUntagged() uses a tagged and
 * untagged probing process, respectively.
 *
 * ChainingHashTableBase supports both serial and concurrent insertions. Both are controlled through
 * a template parameter to ChainingHashTableBase::Insert*() methods.
 *
 * Note 1: ChainingHashTables only store pointers into externally managed storage; it does not
 *         manage any hash table data internally. In other words, the memory of all inserted
 *         HashTableEntry must be owned by an external entity whose lifetime exceeds this
 *         ChainingHashTable!
 * Note 2: ChainingHashTable leverages the ‘next’ pointer in HashTableEntry::next to implement the
 *         linked list bucket chain.
 */
class ChainingHashTableBase {
 private:
  /** X86_64 has 48-bit VM address space, leaving 16 for us to re-purpose. */
  static constexpr uint32_t NUM_TAG_BITS = 16;
  /** The number of bits to use for the physical pointer. */
  static constexpr uint32_t NUM_POINTER_BITS = (sizeof(void *) * common::Constants::K_BITS_PER_BYTE) - NUM_TAG_BITS;
  /** The mask to use to retrieve the physical pointer from a tagged pointer. */
  static constexpr uint64_t MASK_POINTER = (~0ull) >> NUM_TAG_BITS;
  /** The mask to use to retrieve the tag from a tagged pointer. */
  static constexpr uint64_t MASK_TAG = (~0ull) << NUM_POINTER_BITS;
  /** The minimum table size. */
  static constexpr uint64_t MIN_TABLE_SIZE = 8;

 public:
  /** The default load factor to use. */
  static constexpr float DEFAULT_LOAD_FACTOR = 0.7;

  /**
   * This class cannot be copied or moved.
   */
  DISALLOW_COPY_AND_MOVE(ChainingHashTableBase);

  /**
   * Destructor.
   */
  virtual ~ChainingHashTableBase();

  /**
   * Explicitly set the size of the hash table to support at least @em new_size elements. The input
   * size is a lower-bound of the expected number of elements. The sizing operation may compute a
   * larger value to (1) respect the load factor or to (2) ensure a power-of-two size.
   * @param new_size The expected number of elements that will be inserted into the table.
   * @param tracker Memory Tracker
   */
  void SetSize(uint64_t new_size, common::ManagedPointer<MemoryTracker> tracker);

  /**
   * Insert an entry into the hash table without tagging it.
   * @pre The input hash value @em hash should match what's stored in @em entry.
   * @tparam Concurrent Is the insert occurring concurrently with other inserts.
   * @param entry The entry to insert.
   * @param hash The hash value of the entry.
   */
  template <bool Concurrent>
  void InsertUntagged(HashTableEntry *entry, hash_t hash);

  /**
   * Insert an entry into the hash table after updating its tag bits.
   * @pre The input hash value @em hash should match what's stored in @em entry.
   * @tparam Concurrent Is the insert occurring concurrently with other inserts.
   * @param entry The entry to insert.
   * @param hash The hash value of the entry.
   */
  template <bool Concurrent>
  void InsertTagged(HashTableEntry *entry, hash_t hash);

  /**
   * Prefetch the head of the bucket chain for the hash @em hash.
   * @tparam ForRead Whether the prefetch is intended for a subsequent read.
   * @param hash The hash value of the element to prefetch.
   */
  template <bool ForRead>
  void PrefetchChainHead(hash_t hash) const;

  /**
   * Return the head of the bucket chain for a key with the provided hash value. This probe does no
   * leverage tag bits and assumes no concurrent access into the table.
   * @param hash The hash value of the key to find.
   * @return The (potentially null) head of the bucket chain for the given hash.
   */
  HashTableEntry *FindChainHeadUntagged(hash_t hash) const;

  /**
   * Return the head of the bucket chain for a key with the provided hash value. This probe method
   * uses the pointer tagging optimization to determine if the hash value is contained in the table.
   * Assumes no concurrent access into the table.
   * @param hash The hash value of the key to find.
   * @return The (potentially null) head of the bucket chain for the given hash.
   */
  HashTableEntry *FindChainHeadTagged(hash_t hash) const;

  /**
   * @return The total number of bytes this hash table has allocated.
   */
  uint64_t GetTotalMemoryUsage() const { return sizeof(HashTableEntry *) * GetCapacity(); }

  /**
   * @return The size of the main entry directory.
   */
  uint64_t GetCapacity() const { return capacity_; }

  /**
   * @return The configured load factor for the table's directory. Note that this isn't the load
   *         factor value is normally thought of: # elems / # slots. Since this is a bucket-chained
   *         table, load factors can exceed 1.0 if chains are long.
   */
  float GetLoadFactor() const { return load_factor_; }

 protected:
  /** Create an empty hash table. Constructor is protected to ensure base class cannot be instantiated. */
  explicit ChainingHashTableBase(float load_factor) noexcept;

  /** Return the position of the bucket the given hash value lands into. */
  uint64_t BucketPosition(const hash_t hash) const { return hash & mask_; }

  // -------------------------------------------------------
  // Tag-related operations
  // -------------------------------------------------------

  /** Given a tagged HashTableEntry pointer, strip out the tag bits and return an untagged HashTableEntry pointer. */
  static HashTableEntry *UntagPointer(const HashTableEntry *const entry) {
    auto ptr = reinterpret_cast<uintptr_t>(entry);
    return reinterpret_cast<HashTableEntry *>(ptr & MASK_POINTER);
  }

  /** Update the tagged HashTableEntry pointer with the new entry. */
  static HashTableEntry *UpdateTag(const HashTableEntry *const tagged_old_entry,
                                   const HashTableEntry *const untagged_new_entry) {
    auto old_tagged_ptr = reinterpret_cast<uintptr_t>(tagged_old_entry);
    auto new_untagged_ptr = reinterpret_cast<uintptr_t>(untagged_new_entry);
    auto new_tagged_ptr =
        (new_untagged_ptr & MASK_POINTER) | (old_tagged_ptr & MASK_TAG) | TagHash(untagged_new_entry->hash_);
    return reinterpret_cast<HashTableEntry *>(new_tagged_ptr);
  }

  /** Create a tag for the specified hash. */
  static uint64_t TagHash(const hash_t hash) {
    // We use the given hash value to obtain a bit position in the tag to set.
    // We need to extract a signature from the hash value in the range
    // [0, kNumTagBits), so we take the log2(kNumTagBits) most significant bits
    // to determine which bit in the tag to set.
    auto tag_bit_pos = hash >> (sizeof(hash_t) * 8 - 4);
    NOISEPAGE_ASSERT(tag_bit_pos < NUM_TAG_BITS, "Invalid tag!");
    return 1ull << (tag_bit_pos + NUM_POINTER_BITS);
  }

 protected:
  /** Main directory of hash table entry buckets. */
  HashTableEntry **entries_;
  /** The mask to use to compute the bucket position of an entry. */
  uint64_t mask_;
  /** The capacity of the directory. */
  uint64_t capacity_;
  /** The configured load-factor. */
  float load_factor_;
};

template <bool ForRead>
inline void ChainingHashTableBase::PrefetchChainHead(hash_t hash) const {
  const uint64_t pos = BucketPosition(hash);
  util::Memory::Prefetch<ForRead, Locality::Low>(entries_ + pos);
}

#define COMPARE_EXCHANGE_WEAK(ADDRESS, EXPECTED, NEW_VAL)                                      \
  (__atomic_compare_exchange_n((ADDRESS),        /* Address to atomically CAS into. */         \
                               (EXPECTED),       /* The old value we read from the address. */ \
                               (NEW_VAL),        /* The new value we want to write there. */   \
                               true,             /* Weak exchange? Yes, for performance. */    \
                               __ATOMIC_RELEASE, /* Use release semantics for success */       \
                               __ATOMIC_RELAXED  /* Use relaxed semantics for failure */       \
                               ))                // NOLINT

template <bool Concurrent>
inline void ChainingHashTableBase::InsertUntagged(HashTableEntry *const entry, const hash_t hash) {
  const uint64_t pos = BucketPosition(hash);

  NOISEPAGE_ASSERT(pos < GetCapacity(), "Computed table position exceeds capacity!");
  NOISEPAGE_ASSERT(entry->hash_ == hash, "Hash value not set in entry!");

  if constexpr (Concurrent) {
    HashTableEntry *old_entry = entries_[pos];
    do {
      entry->next_ = old_entry;
    } while (!COMPARE_EXCHANGE_WEAK(&entries_[pos], &old_entry, entry));
  } else {  // NOLINT
    entry->next_ = entries_[pos];
    entries_[pos] = entry;
  }
}

template <bool Concurrent>
inline void ChainingHashTableBase::InsertTagged(HashTableEntry *const entry, const hash_t hash) {
  const uint64_t pos = BucketPosition(hash);

  NOISEPAGE_ASSERT(pos < GetCapacity(), "Computed table position exceeds capacity!");
  NOISEPAGE_ASSERT(entry->hash_ == hash, "Hash value not set in entry!");

  if constexpr (Concurrent) {  // NOLINT
    HashTableEntry *old_entry = entries_[pos];
    HashTableEntry *new_entry = nullptr;
    do {
      entry->next_ = UntagPointer(old_entry);   // Un-tag the old entry and link.
      new_entry = UpdateTag(old_entry, entry);  // Tag the new entry.
    } while (!COMPARE_EXCHANGE_WEAK(&entries_[pos], &old_entry, new_entry));
  } else {  // NOLINT
    entry->next_ = UntagPointer(entries_[pos]);
    entries_[pos] = UpdateTag(entries_[pos], entry);
  }
}

#undef COMPARE_EXCHANGE_WEAK

inline HashTableEntry *ChainingHashTableBase::FindChainHeadUntagged(hash_t hash) const {
  const uint64_t pos = BucketPosition(hash);
  return entries_[pos];
}

inline HashTableEntry *ChainingHashTableBase::FindChainHeadTagged(hash_t hash) const {
  const HashTableEntry *const candidate = FindChainHeadUntagged(hash);
  auto exists_in_chain = reinterpret_cast<uintptr_t>(candidate) & TagHash(hash);
  return (exists_in_chain != 0u ? UntagPointer(candidate) : nullptr);
}

//===----------------------------------------------------------------------===//
//
// Chaining Hash Table
//
//===----------------------------------------------------------------------===//

/**
 * The main chaining hash table implementation.
 * @tparam UseTags Boolean flag indicating whether to use the pointer tagging optimizations.
 */
template <bool UseTags>
class ChainingHashTable : public ChainingHashTableBase {
  // clang-format off
  template <bool> friend class ChainingHashTableIterator;
  template <bool> friend class ChainingHashTableVectorIterator;
  // clang-format on

 public:
  /**
   * Create an empty hash table. Users must first call SetSize() before use.
   * @param load_factor The desired load factor.
   */
  explicit ChainingHashTable(float load_factor = DEFAULT_LOAD_FACTOR);

  /**
   * Explicitly set the size of the hash table to support at least @em new_size elements. The input
   * size is a lower-bound of the expected number of elements. The sizing operation may compute a
   * larger value to (1) respect the load factor or to (2) ensure a power-of-two size. Also resets
   * the element count.
   * @param new_size The expected number of elements that will be inserted into the table.
   * @param tracker Memory Tracker
   */
  void SetSize(uint64_t new_size, common::ManagedPointer<MemoryTracker> tracker);

  /**
   * Insert the given hash table entry into this hash table.
   * @pre The hash value for the entry must already be computed and stored in entry->hash.
   * @tparam Concurrent Is the insert occurring concurrently with other inserts.
   * @param entry The entry to insert.
   */
  template <bool Concurrent>
  void Insert(HashTableEntry *entry);

  /**
   * Insert all entries in the given vector into this hash table. All entries must have their hash
   * values already computed.
   * @pre All hash values must have been computed already.
   * @tparam Concurrent Is the insert occurring concurrently with other inserts.
   * @tparam Allocator The allocator the vector uses. Templated to allow different vectors.
   * @param entries The list of entries to insert.
   */
  template <bool Concurrent, typename Allocator>
  void InsertBatch(util::ChunkedVector<Allocator> *entries);

  /**
   * Return the head of the bucket chain for a key with the provided hash value. Probing assumes no
   * concurrent modifications to the hash table. Thus, is suitable for WORM based workloads.
   * @param hash The hash value of the key to find.
   * @return The (potentially null) head of the bucket chain for the given hash.
   */
  HashTableEntry *FindChainHead(hash_t hash) const;

  /**
   * Empty all entries in this hash table into the sink functor. After this function exits, the hash
   * table is empty.
   * @tparam F The function must be of the form void(*)(HashTableEntry*)
   * @param sink The sink of all entries in the hash table
   */
  template <typename F>
  void FlushEntries(F &&sink);

  /**
   * @return Collect and return a tuple containing the minimum, maximum, and average bucket
   * chain in this hash table. This is not a concurrent operation!
   */
  std::tuple<uint64_t, uint64_t, float> GetChainLengthStats() const;

  /**
   * @return True if the hash table is empty; false otherwise.
   */
  bool IsEmpty() const { return GetElementCount() == 0; }

  /**
   * @return The number of elements stored in this hash table.
   */
  uint64_t GetElementCount() const { return num_elements_.load(std::memory_order_relaxed); }

 private:
  // Add the given value to the total element count.
  void AddElementCount(uint64_t v) { num_elements_.fetch_add(v, std::memory_order_relaxed); }

  // Bulk insertion with configurable pre-fetching.
  template <bool Prefetch, bool Concurrent, typename Allocator>
  void InsertBatchInternal(util::ChunkedVector<Allocator> *entries);

 private:
  // The current number of elements stored in the table.
  std::atomic<uint64_t> num_elements_;
};

template <bool UseTags>
template <bool Concurrent>
inline void ChainingHashTable<UseTags>::Insert(HashTableEntry *entry) {
  if constexpr (UseTags) {  // NOLINT
    InsertTagged<Concurrent>(entry, entry->hash_);
  } else {  // NOLINT
    InsertUntagged<Concurrent>(entry, entry->hash_);
  }

  // Update element count.
  AddElementCount(1);
}

template <bool UseTags>
template <bool Prefetch, bool Concurrent, typename Allocator>
inline void ChainingHashTable<UseTags>::InsertBatchInternal(util::ChunkedVector<Allocator> *entries) {
  const uint64_t size = entries->size();
  for (uint64_t idx = 0, prefetch_idx = common::Constants::K_PREFETCH_DISTANCE; idx < size; idx++, prefetch_idx++) {
    if constexpr (Prefetch) {  // NOLINT
      if (LIKELY(prefetch_idx < size)) {
        auto *prefetch_entry = reinterpret_cast<HashTableEntry *>((*entries)[prefetch_idx]);
        PrefetchChainHead<false>(prefetch_entry->hash_);
      }
    }

    auto *entry = reinterpret_cast<HashTableEntry *>((*entries)[idx]);
    if constexpr (UseTags) {  // NOLINT
      InsertTagged<Concurrent>(entry, entry->hash_);
    } else {
      InsertUntagged<Concurrent>(entry, entry->hash_);
    }
  }
}

template <bool UseTags>
template <bool Concurrent, typename Allocator>
inline void ChainingHashTable<UseTags>::InsertBatch(util::ChunkedVector<Allocator> *entries) {
  const uint64_t l3_size = CpuInfo::Instance()->GetCacheSize(CpuInfo::L3_CACHE);
  if (bool out_of_cache = GetTotalMemoryUsage() > l3_size; out_of_cache) {
    InsertBatchInternal<true, Concurrent>(entries);
  } else {
    InsertBatchInternal<false, Concurrent>(entries);
  }

  // Update element count.
  AddElementCount(entries->size());
}

template <bool UseTags>
inline HashTableEntry *ChainingHashTable<UseTags>::FindChainHead(hash_t hash) const {
  if constexpr (UseTags) {  // NOLINT
    return FindChainHeadTagged(hash);
  } else {  // NOLINT
    return FindChainHeadUntagged(hash);
  }
}

template <bool UseTags>
template <typename F>
inline void ChainingHashTable<UseTags>::FlushEntries(F &&sink) {
  static_assert(std::is_invocable_v<F, HashTableEntry *>);

  for (uint64_t idx = 0; idx < capacity_; idx++) {
    HashTableEntry *entry = entries_[idx];

    if constexpr (UseTags) {  // NOLINT
      entry = UntagPointer(entry);
    }

    while (entry != nullptr) {
      HashTableEntry *next = entry->next_;
      sink(entry);
      entry = next;
    }

    entries_[idx] = nullptr;
  }

  num_elements_ = 0;
}

// Useful aliases.
using TaggedChainingHashTable = ChainingHashTable<true>;
using UntaggedChainingHashTable = ChainingHashTable<false>;

//===----------------------------------------------------------------------===//
//
// Chaining Hash Table Iterator
//
//===----------------------------------------------------------------------===//

/**
 * An iterator over the entries in a generic hash table. It's assumed that the underlying hash table
 * is not modified during iteration. This is mostly true for SQL processing where the hash tables
 * are WORM structures.
 * @tparam UseTag Should the iterator use tagged reads?
 */
template <bool UseTag>
class ChainingHashTableIterator {
 public:
  /**
   * Construct an iterator over the given hash table @em table.
   * @param table The table to iterate over.
   */
  explicit ChainingHashTableIterator(const ChainingHashTable<UseTag> &table) noexcept
      : table_(table), entries_index_(0), curr_entry_(nullptr) {
    Next();
  }

  /**
   * @return True if there is more data in the iterator; false otherwise.
   */
  bool HasNext() const noexcept { return curr_entry_ != nullptr; }

  /**
   * Advance the iterator one element.
   */
  void Next() noexcept {
    // If the current entry has a next link, use that
    if (curr_entry_ != nullptr) {
      curr_entry_ = curr_entry_->next_;
      if (curr_entry_ != nullptr) {
        return;
      }
    }

    // While we haven't exhausted the directory, and haven't found a valid entry
    // continue on ...
    while (entries_index_ < table_.GetCapacity()) {
      curr_entry_ = table_.entries_[entries_index_++];

      if constexpr (UseTag) {  // NOLINT
        curr_entry_ = ChainingHashTable<UseTag>::UntagPointer(curr_entry_);
      }

      if (curr_entry_ != nullptr) {
        return;
      }
    }
  }

  /**
   * @return The element the iterator is currently pointing to.
   */
  const HashTableEntry *GetCurrentEntry() const noexcept { return curr_entry_; }

 private:
  // The table we're iterating over
  const ChainingHashTable<UseTag> &table_;
  // The index into the hash table's entries directory to read from next
  uint64_t entries_index_;
  // The current entry the iterator is pointing to
  const HashTableEntry *curr_entry_;
};

//===----------------------------------------------------------------------===//
//
// Chaining Hash Table Vector Iterator
//
//===----------------------------------------------------------------------===//

/**
 * An iterator over a generic hash table that works vector-at-a-time. It's assumed that the
 * underlying hash table is not modified during iteration. This is mostly true for SQL processing
 * where the hash tables are WORM structures.
 * @tparam UseTag Should the iterator use tagged reads?
 */
// TODO(pmenon): Fix my performance
template <bool UseTag>
class ChainingHashTableVectorIterator {
 public:
  /**
   * Construct an iterator over the given hash table @em table.
   * @param table The table to iterate over.
   * @param memory The memory pool to use for allocations.
   */
  ChainingHashTableVectorIterator(const ChainingHashTable<UseTag> &table, MemoryPool *memory) noexcept;

  /**
   * Deallocate the entry cache array
   */
  ~ChainingHashTableVectorIterator();

  /**
   * @return True if there's more data in the iterator; false otherwise.
   */
  bool HasNext() const { return entry_vec_end_idx_ > 0; }

  /**
   * Advance the iterator by once vector's worth of data.
   */
  void Next();

  /**
   * @return The current batch of entries and its size.
   */
  std::pair<uint16_t, const HashTableEntry **> GetCurrentBatch() const {
    return std::make_pair(entry_vec_end_idx_, entry_vec_);
  }

 private:
  // Pool to use for memory allocations
  MemoryPool *memory_;

  // The hash table we're iterating over
  const ChainingHashTable<UseTag> &table_;

  // The index into the hash table's entries directory to read from next
  uint64_t table_dir_index_;

  // The temporary cache of valid entries, and indexes into the entry cache
  // pointing to the current and last valid entry.
  const HashTableEntry **entry_vec_;
  uint32_t entry_vec_end_idx_;
};

}  // namespace noisepage::execution::sql
