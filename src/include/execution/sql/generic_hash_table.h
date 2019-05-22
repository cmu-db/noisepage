#pragma once

#include <atomic>

#include "execution/sql/hash_table_entry.h"
#include "execution/util/common.h"
#include "execution/util/macros.h"
#include "execution/util/memory.h"

namespace tpl::sql {

/// GenericHashTable serves as a dead-simple hash table for joins and
/// aggregations in TPL. It is a generic bytes-to-bytes hash table implemented
/// as a bucket-chained table with pointer tagging. Pointer tagging uses the
/// first \a kNumTagBits bits of the entry pointers in the main bucket directory
/// as a bloom filter. It optionally supports concurrent inserts (and trivially
/// concurrent probes). This class only stores pointers into externally managed
/// storage, it does not store any hash table data internally at all.
///
/// Note that this class makes use of the \a HashTableEntry::next pointer to
/// implement the linked list bucket chain.
class GenericHashTable {
 private:
  static constexpr const u32 kNumTagBits = 16;
  static constexpr const u32 kNumPointerBits = sizeof(u8 *) * 8 - kNumTagBits;
  static constexpr const u64 kMaskPointer = (~0ull) >> kNumTagBits;
  static constexpr const u64 kMaskTag = (~0ull) << kNumPointerBits;

 public:
  /// Constructor does not allocate memory. Callers must first call SetSize()
  /// before using this hash map.
  /// \param load_factor The desired load-factor for the table
  explicit GenericHashTable(float load_factor = 0.7f) noexcept;

  /// Cleanup
  ~GenericHashTable();

  /// This class cannot be copied or moved
  DISALLOW_COPY_AND_MOVE(GenericHashTable);

  /// Insert an entry into the hash table, ignoring tagging the pointer into the
  /// bucket head
  /// \tparam Concurrent Is the insert occurring concurrently with other inserts
  /// \param[in] new_entry The entry to insert
  /// \param[in] hash The hash value of the entry
  template <bool Concurrent>
  void Insert(HashTableEntry *new_entry, hash_t hash);

  /// Insert an entry into the hash table, updating the tag in the bucket head
  /// \tparam Concurrent Is the insert occurring concurrently with other inserts
  /// \param[in] new_entry The entry to insert
  /// \param[in] hash The hash value of the entry
  template <bool Concurrent>
  void InsertTagged(HashTableEntry *new_entry, hash_t hash);

  /// Explicitly set the size of the hash map
  /// \param[in] new_size The expected number of elements to size the table for
  void SetSize(u64 new_size);

  /// Prefetch the head of the bucket chain for the hash \a hash
  template <bool ForRead>
  void PrefetchChainHead(hash_t hash) const;

  /// Given a hash value, return the head of the bucket chain ignoring any tag.
  /// This probe is performed assuming no concurrent access into the table.
  /// \param[in] hash The hash value of the element to find
  /// \return The (potentially null) head of the bucket chain for the given hash
  HashTableEntry *FindChainHead(hash_t hash) const;

  /// Given a hash value, return the head of the bucket chain removing the tag.
  /// This probe is performed assuming no concurrent access into the table.
  /// \param[in] hash The hash value of the element to find
  /// \return The (potentially null) head of the bucket chain for the given hash
  HashTableEntry *FindChainHeadWithTag(hash_t hash) const;

  /// Return the number of bytes this hash table has allocated
  u64 GetTotalMemoryUsage() const { return sizeof(HashTableEntry *) * capacity(); }

  /// Return the number of elements stored in this hash table
  u64 num_elements() const { return num_elems_; }

  /// Return the maximum capacity of this hash table in number of elements
  u64 capacity() const { return capacity_; }

 private:
  // -------------------------------------------------------
  // Tag-related operations
  // -------------------------------------------------------

  // Given a tagged HashTableEntry pointer, strip out the tag bits and return an
  // untagged HashTableEntry pointer
  static HashTableEntry *UntagPointer(const HashTableEntry *const entry) {
    auto ptr = reinterpret_cast<intptr_t>(entry);
    return reinterpret_cast<HashTableEntry *>(ptr & kMaskPointer);
  }

  static HashTableEntry *UpdateTag(const HashTableEntry *const tagged_old_entry,
                                   const HashTableEntry *const untagged_new_entry) {
    auto old_tagged_ptr = reinterpret_cast<intptr_t>(tagged_old_entry);
    auto new_untagged_ptr = reinterpret_cast<intptr_t>(untagged_new_entry);
    auto new_tagged_ptr =
        (new_untagged_ptr & kMaskPointer) | (old_tagged_ptr & kMaskTag) | TagHash(untagged_new_entry->hash);
    return reinterpret_cast<HashTableEntry *>(new_tagged_ptr);
  }

  static u64 TagHash(const hash_t hash) {
    // We use the given hash value to obtain a bit position in the tag to set.
    // Thus, we need to extract a sample/signature from the hash value in the
    // range [0, kNumTagBits), so we take the log2(kNumTagBits) most significant
    // bits to determine which bit in the tag to set.
    auto tag_bit_pos = hash >> (sizeof(hash_t) * 8 - 4);
    TPL_ASSERT(tag_bit_pos < kNumTagBits, "Invalid tag!");
    return 1ull << (tag_bit_pos + kNumPointerBits);
  }

 private:
  // Main bucket table
  std::atomic<HashTableEntry *> *entries_{nullptr};

  // The mask to use to determine the bucket position of an entry given its hash
  u64 mask_{0};

  // The capacity of the directory
  u64 capacity_{0};

  // The current number of elements stored in the table
  u64 num_elems_{0};

  // The current load-factor
  float load_factor_;
};

// ---------------------------------------------------------
// Implementation below
// ---------------------------------------------------------

template <bool ForRead>
void GenericHashTable::PrefetchChainHead(hash_t hash) const {
  const u64 pos = hash & mask_;
  util::Prefetch<ForRead, Locality::Low>(entries_ + pos);
}

inline HashTableEntry *GenericHashTable::FindChainHead(hash_t hash) const {
  const u64 pos = hash & mask_;
  return entries_[pos].load(std::memory_order_relaxed);
}

inline HashTableEntry *GenericHashTable::FindChainHeadWithTag(hash_t hash) const {
  const HashTableEntry *const candidate = FindChainHead(hash);
  auto exists_in_chain = (reinterpret_cast<intptr_t>(candidate) & TagHash(hash)) != 0;
  return (exists_in_chain ? UntagPointer(candidate) : nullptr);
}

template <bool Concurrent>
inline void GenericHashTable::Insert(HashTableEntry *new_entry, hash_t hash) {
  const auto pos = hash & mask_;

  TPL_ASSERT(pos < capacity(), "Computed table position exceeds capacity!");
  TPL_ASSERT(new_entry->hash == hash, "Hash value not set in entry!");

  if constexpr (Concurrent) {
    std::atomic<HashTableEntry *> &loc = entries_[pos];
    HashTableEntry *old_entry = loc.load();
    do {
      new_entry->next = old_entry;
    } while (!loc.compare_exchange_weak(old_entry, new_entry));
    // clang-tidy complains about bad indentation in the next line
  } else {  // NOLINT
    std::atomic<HashTableEntry *> &loc = entries_[pos];
    HashTableEntry *old_entry = loc.load(std::memory_order_relaxed);
    new_entry->next = old_entry;
    loc.store(new_entry, std::memory_order_relaxed);
  }

  num_elems_++;
}

template <bool Concurrent>
inline void GenericHashTable::InsertTagged(HashTableEntry *new_entry, hash_t hash) {
  const auto pos = hash & mask_;

  TPL_ASSERT(pos < capacity(), "Computed table position exceeds capacity!");
  TPL_ASSERT(new_entry->hash == hash, "Hash value not set in entry!");

  if constexpr (Concurrent) {
    std::atomic<HashTableEntry *> &loc = entries_[pos];
    HashTableEntry *old_entry = loc.load();
    do {
      new_entry->next = UntagPointer(old_entry);
      new_entry = UpdateTag(old_entry, new_entry);
    } while (!loc.compare_exchange_weak(old_entry, new_entry));

  } else {
    std::atomic<HashTableEntry *> &loc = entries_[pos];
    HashTableEntry *old_entry = loc.load(std::memory_order_relaxed);
    new_entry->next = UntagPointer(old_entry);
    loc.store(UpdateTag(old_entry, new_entry), std::memory_order_relaxed);
  }

  num_elems_++;
}

}  // namespace tpl::sql
