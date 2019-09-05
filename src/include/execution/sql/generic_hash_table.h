#pragma once

#include <atomic>

#include "common/constants.h"
#include "common/macros.h"
#include "execution/sql/hash_table_entry.h"
#include "execution/sql/memory_pool.h"
#include "execution/util/execution_common.h"
#include "execution/util/memory.h"

namespace terrier::execution::sql {

/**
 * GenericHashTable serves as a dead-simple hash table for joins and
 * aggregations in TPL. It is a generic bytes-to-bytes hash table implemented
 * as a bucket-chained table with pointer tagging. Pointer tagging uses the
 * first @em GenericHashTable::kNumTagBits bits of the entry pointers in the
 * main bucket directory as a bloom filter. It optionally supports concurrent
 * inserts (and trivially concurrent probes). This class only stores pointers
 * into externally managed storage, it does not store any hash table data
 * internally at all.
 *
 * Note that this class makes use of the @em HashTableEntry::next pointer to
 * implement the linked list bucket chain.
 */
class GenericHashTable {
 private:
  static constexpr const uint32_t K_NUM_TAG_BITS = 16;
  static constexpr const uint32_t K_NUM_POINTER_BITS = sizeof(uint8_t *) * 8 - K_NUM_TAG_BITS;
  static constexpr const uint64_t K_MASK_POINTER = (~0ull) >> K_NUM_TAG_BITS;
  static constexpr const uint64_t K_MASK_TAG = (~0ull) << K_NUM_POINTER_BITS;

 public:
  /**
   * Constructor does not allocate memory. Callers must first call SetSize()
   * before using this hash map.
   * @param load_factor The desired load-factor for the table
   */
  explicit GenericHashTable(float load_factor = 0.7f) noexcept;

  /**
   * Cleanup.
   */
  ~GenericHashTable();

  /**
   * This class cannot be copied or moved.
   */
  DISALLOW_COPY_AND_MOVE(GenericHashTable);

  /**
   * Insert an entry into the hash table, ignoring tagging the pointer into the
   * bucket head
   * @tparam Concurrent Is the insert occurring concurrently with other inserts
   * @param new_entry The entry to insert
   * @param hash The hash value of the entry
   */
  template <bool Concurrent>
  void Insert(HashTableEntry *new_entry, hash_t hash);

  /**
   * Insert an entry into the hash table, updating the tag in the bucket head
   * @tparam Concurrent Is the insert occurring concurrently with other inserts
   * @param new_entry The entry to insert
   * @param hash The hash value of the entry
   */
  template <bool Concurrent>
  void InsertTagged(HashTableEntry *new_entry, hash_t hash);

  /**
   * Explicitly set the size of the hash table to support at least @em new_size
   * elements with good performance.
   * @param new_size The expected number of elements to size the table for
   */
  void SetSize(uint64_t new_size);

  /**
   * Prefetch the head of the bucket chain for the hash \a hash
   * @tparam ForRead Whether the prefetch is intended for a subsequent read op
   * @param hash The hash value of the element to prefetch
   */
  template <bool ForRead>
  void PrefetchChainHead(hash_t hash) const;

  /**
   * Given a hash value, return the head of the bucket chain ignoring any tag.
   * This probe is performed assuming no concurrent access into the table.
   * @param hash The hash value of the element to find
   * @return The (potentially null) head of the bucket chain for the given hash
   */
  HashTableEntry *FindChainHead(hash_t hash) const;

  /**
   * Given a hash value, return the head of the bucket chain removing the tag.
   * This probe is performed assuming no concurrent access into the table.
   * @param hash The hash value of the element to find
   * @return The (potentially null) head of the bucket chain for the given hash
   */
  HashTableEntry *FindChainHeadWithTag(hash_t hash) const;

  /**
   * Empty all entries in this hash table into the sink functor. After this
   * function exits, the hash table is empty.
   * @tparam F The function must be of the form void(*)(HashTableEntry*)
   * @param sink The sink of all entries in the hash table
   */
  template <typename F>
  void FlushEntries(const F &sink);

  /**
   * Return the total number of bytes this hash table has allocated
   */
  uint64_t GetTotalMemoryUsage() const { return sizeof(HashTableEntry *) * Capacity(); }

  /**
   * Return the number of elements stored in this hash table
   */
  uint64_t NumElements() const { return num_elems_; }

  /**
   * Return the maximum number of elements this hash table can store at its
   * current size
   */
  uint64_t Capacity() const { return capacity_; }

  /**
   * The configured load factor for the table's directory. Note that this isn't
   * the load factor value is normally thought of: # elems / # slots. Since
   * this is a bucket-chained table, load factors can exceed 1.0 if chains are
   * long.
   */
  float LoadFactor() const { return load_factor_; }

 private:
  template <bool UseTag>
  friend class GenericHashTableIterator;
  template <bool UseTag>
  friend class GenericHashTableVectorIterator;

  // -------------------------------------------------------
  // Tag-related operations
  // -------------------------------------------------------

  // Given a tagged HashTableEntry pointer, strip out the tag bits and return an
  // untagged HashTableEntry pointer
  static HashTableEntry *UntagPointer(const HashTableEntry *const entry) {
    auto ptr = reinterpret_cast<intptr_t>(entry);
    return reinterpret_cast<HashTableEntry *>(ptr & K_MASK_POINTER);
  }

  static HashTableEntry *UpdateTag(const HashTableEntry *const tagged_old_entry,
                                   const HashTableEntry *const untagged_new_entry) {
    auto old_tagged_ptr = reinterpret_cast<intptr_t>(tagged_old_entry);
    auto new_untagged_ptr = reinterpret_cast<intptr_t>(untagged_new_entry);
    auto new_tagged_ptr =
        (new_untagged_ptr & K_MASK_POINTER) | (old_tagged_ptr & K_MASK_TAG) | TagHash(untagged_new_entry->hash_);
    return reinterpret_cast<HashTableEntry *>(new_tagged_ptr);
  }

  static uint64_t TagHash(const hash_t hash) {
    // We use the given hash value to obtain a bit position in the tag to set.
    // Thus, we need to extract a sample/signature from the hash value in the
    // range [0, kNumTagBits), so we take the log2(kNumTagBits) most significant
    // bits to determine which bit in the tag to set.
    auto tag_bit_pos = hash >> (sizeof(hash_t) * 8 - 4);
    TERRIER_ASSERT(tag_bit_pos < K_NUM_TAG_BITS, "Invalid tag!");
    return 1ull << (tag_bit_pos + K_NUM_POINTER_BITS);
  }

 private:
  // Main bucket table
  std::atomic<HashTableEntry *> *entries_{nullptr};

  // The mask to use to determine the bucket position of an entry given its hash
  uint64_t mask_{0};

  // The capacity of the directory
  uint64_t capacity_{0};

  // The current number of elements stored in the table
  uint64_t num_elems_{0};

  // The current load-factor
  float load_factor_;
};

// ---------------------------------------------------------
// Implementation below
// ---------------------------------------------------------

template <bool ForRead>
void GenericHashTable::PrefetchChainHead(hash_t hash) const {
  const uint64_t pos = hash & mask_;
  util::Prefetch<ForRead, Locality::Low>(entries_ + pos);
}

inline HashTableEntry *GenericHashTable::FindChainHead(hash_t hash) const {
  const uint64_t pos = hash & mask_;
  return entries_[pos].load(std::memory_order_relaxed);
}

inline HashTableEntry *GenericHashTable::FindChainHeadWithTag(hash_t hash) const {
  const HashTableEntry *const candidate = FindChainHead(hash);
  auto exists_in_chain = reinterpret_cast<intptr_t>(candidate) & TagHash(hash);
  return (static_cast<bool>(exists_in_chain) ? UntagPointer(candidate) : nullptr);
}

template <bool Concurrent>
inline void GenericHashTable::Insert(HashTableEntry *new_entry, hash_t hash) {
  const auto pos = hash & mask_;

  TERRIER_ASSERT(pos < Capacity(), "Computed table position exceeds capacity!");
  TERRIER_ASSERT(new_entry->hash_ == hash, "Hash value not set in entry!");

  if constexpr (Concurrent) {
    std::atomic<HashTableEntry *> &loc = entries_[pos];
    HashTableEntry *old_entry = loc.load();
    do {
      new_entry->next_ = old_entry;
    } while (!loc.compare_exchange_weak(old_entry, new_entry));
  } else {  // NOLINT
    std::atomic<HashTableEntry *> &loc = entries_[pos];
    HashTableEntry *old_entry = loc.load(std::memory_order_relaxed);
    new_entry->next_ = old_entry;
    loc.store(new_entry, std::memory_order_relaxed);
  }

  num_elems_++;
}

template <bool Concurrent>
inline void GenericHashTable::InsertTagged(HashTableEntry *new_entry, hash_t hash) {
  const auto pos = hash & mask_;

  TERRIER_ASSERT(pos < Capacity(), "Computed table position exceeds capacity!");
  TERRIER_ASSERT(new_entry->hash_ == hash, "Hash value not set in entry!");

  if constexpr (Concurrent) {
    std::atomic<HashTableEntry *> &loc = entries_[pos];
    HashTableEntry *old_entry = loc.load();
    do {
      new_entry->next_ = UntagPointer(old_entry);
      new_entry = UpdateTag(old_entry, new_entry);
    } while (!loc.compare_exchange_weak(old_entry, new_entry));

  } else {  // NOLINT
    std::atomic<HashTableEntry *> &loc = entries_[pos];
    HashTableEntry *old_entry = loc.load(std::memory_order_relaxed);
    new_entry->next_ = UntagPointer(old_entry);
    loc.store(UpdateTag(old_entry, new_entry), std::memory_order_relaxed);
  }

  num_elems_++;
}

template <typename F>
inline void GenericHashTable::FlushEntries(const F &sink) {
  static_assert(std::is_invocable_v<F, HashTableEntry *>);

  for (uint32_t idx = 0; idx < capacity_; idx++) {
    HashTableEntry *entry = entries_[idx].load(std::memory_order_relaxed);
    while (entry != nullptr) {
      HashTableEntry *next = entry->next_;
      sink(entry);
      entry = next;
    }
    entries_[idx].store(nullptr, std::memory_order_relaxed);
  }

  num_elems_ = 0;
}

// ---------------------------------------------------------
// Generic Hash Table Iterator
// ---------------------------------------------------------

/**
 * An iterator over the entries in a generic hash table.
 * @tparam UseTag Should the iterator use tagged reads?
 */
template <bool UseTag>
class GenericHashTableIterator {
 public:
  /**
   * Construct an iterator over the given hash table @em table.
   * @param table The table to iterate over.
   */
  explicit GenericHashTableIterator(const GenericHashTable &table) noexcept
      : table_(table), entries_index_(0), curr_entry_(nullptr) {
    Next();
  }

  /**
   * Is there more data in the iterator?
   */
  bool HasNext() const noexcept { return curr_entry_ != nullptr; }

  /**
   * Advance the iterator one element.
   */
  void Next() noexcept;

  /**
   * Access the element the iterator is currently pointing to.
   */
  const HashTableEntry *GetCurrentEntry() const noexcept { return curr_entry_; }

 private:
  // The table we're iterating over
  const GenericHashTable &table_;
  // The index into the hash table's entries directory to read from next
  uint64_t entries_index_;
  // The current entry the iterator is pointing to
  const HashTableEntry *curr_entry_;
};

template <bool UseTag>
inline void GenericHashTableIterator<UseTag>::Next() noexcept {
  // If the current entry has a next link, use that
  if (curr_entry_ != nullptr) {
    curr_entry_ = curr_entry_->next_;
    if (curr_entry_ != nullptr) {
      return;
    }
  }

  // While we haven't exhausted the directory, and haven't found a valid entry
  // continue on ...
  while (entries_index_ < table_.Capacity()) {
    curr_entry_ = table_.entries_[entries_index_++].load(std::memory_order_relaxed);

    // NOLINTNEXTLINE: bugprone-suspicious-semicolon: seems like a false positive because of constexpr
    if constexpr (UseTag) {
      curr_entry_ = GenericHashTable::UntagPointer(curr_entry_);
    }

    if (curr_entry_ != nullptr) {
      return;
    }
  }
}

// ---------------------------------------------------------
// Generic Hash Table Vector Iterator
// ---------------------------------------------------------

/**
 * An iterator over a generic hash table that works vector-at-a-time.
 * @tparam UseTag Should the iterator use tagged reads?
 */
// TODO(pmenon): Fix my performance
template <bool UseTag>
class GenericHashTableVectorIterator {
 public:
  /**
   * Construct an iterator over the given hash table @em table.
   * @param table The table to iterate over.
   * @param memory The memory pool to use for allocations
   */
  GenericHashTableVectorIterator(const GenericHashTable &table, MemoryPool *memory) noexcept;

  /**
   * Deallocate the entry cache array
   */
  ~GenericHashTableVectorIterator();

  /**
   * Is there more data in the iterator?
   */
  bool HasNext() const noexcept { return entry_vec_idx_ < entry_vec_end_idx_; }

  /**
   * Advance the iterator one element.
   */
  void Next() noexcept;

  /**
   * Access the element the iterator is currently pointing to.
   */
  const HashTableEntry *GetCurrentEntry() const noexcept { return entry_vec_[entry_vec_idx_]; }

 private:
  void Refill();

 private:
  // The hash table we're iterating over
  const GenericHashTable &table_;
  // Pool to use for memory allocations
  MemoryPool *memory_;
  // The temporary cache of valid entries
  const HashTableEntry **entry_vec_;
  // The index into the hash table's entries directory to read from next
  uint64_t entries_index_;
  const HashTableEntry *next_;
  // The index into the entry cache the iterator is pointing to
  uint16_t entry_vec_idx_;
  // The number of valid entries in the entry cache
  uint16_t entry_vec_end_idx_;
};

template <bool UseTag>
inline GenericHashTableVectorIterator<UseTag>::GenericHashTableVectorIterator(const GenericHashTable &table,
                                                                              MemoryPool *memory) noexcept
    : table_(table),
      memory_(memory),
      entry_vec_(memory_->AllocateArray<const HashTableEntry *>(common::Constants::K_DEFAULT_VECTOR_SIZE,
                                                                common::Constants::CACHELINE_SIZE, true)),
      entries_index_(0),
      next_(nullptr),
      entry_vec_idx_(0),
      entry_vec_end_idx_(0) {
  Refill();
}

template <bool UseTag>
inline GenericHashTableVectorIterator<UseTag>::~GenericHashTableVectorIterator() {
  memory_->DeallocateArray(entry_vec_, common::Constants::K_DEFAULT_VECTOR_SIZE);
}

template <bool UseTag>
inline void GenericHashTableVectorIterator<UseTag>::Next() noexcept {
  if (++entry_vec_idx_ >= entry_vec_end_idx_) {
    Refill();
  }
}

template <bool UseTag>
inline void GenericHashTableVectorIterator<UseTag>::Refill() {
  // Reset
  entry_vec_idx_ = entry_vec_end_idx_ = 0;

  while (true) {
    // While we're in the middle of a bucket chain and we have room to insert
    // new entries, continue along the bucket chain.
    while (next_ != nullptr && entry_vec_end_idx_ < common::Constants::K_DEFAULT_VECTOR_SIZE) {
      entry_vec_[entry_vec_end_idx_++] = next_;
      next_ = next_->next_;
    }

    // If we've filled up the entries buffer, drop out
    if (entry_vec_end_idx_ == common::Constants::K_DEFAULT_VECTOR_SIZE) {
      return;
    }

    // If we've exhausted the hash table, drop out
    if (entries_index_ == table_.Capacity()) {
      return;
    }

    // Move to next bucket
    next_ = table_.entries_[entries_index_++].load(std::memory_order_relaxed);
    // NOLINTNEXTLINE: bugprone-suspicious-semicolon: seems like a false positive because of constexpr
    if constexpr (UseTag) {
      next_ = GenericHashTable::UntagPointer(next_);
    }
  }
}

}  // namespace terrier::execution::sql
