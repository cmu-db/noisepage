#pragma once

#include <type_traits>

#include "execution/sql/sql.h"

namespace noisepage::execution::sql {

using ConciseHashTableSlot = uint64_t;

/**
 * A generic structure used to represent an entry in either a chaining hash table or a concise hash table.
 * An entry is a variably-sized chunk of memory where the keys, attributes, and aggregates are
 * stored in the @em payload field. This structure is used for both joins and aggregations.
 */
struct HashTableEntry {
  union {
    /** Used to chain together entries falling to the same bucket within a chaining hash table. */
    HashTableEntry *next_;
    /** Used to record the slot this entry occupies in a concise hash table. */
    ConciseHashTableSlot cht_slot_;
    /** Used during overflow entry reordering when building a concise hash table. */
    uint64_t overflow_count_;
  };

  /** Hash value. */
  hash_t hash_;
  /** Tuple. */
  byte payload_[0];

  /** @return Total size in bytes of a hash table entry element that stores a payload of the given size (in bytes). */
  static constexpr std::size_t ComputeEntrySize(const std::size_t payload_size) {
    return sizeof(HashTableEntry) + payload_size;
  }

  /** @return The byte offset of the payload within a hash table entry element. */
  static constexpr std::size_t ComputePayloadOffset() { return sizeof(HashTableEntry); }

  /** @return A typed pointer to the payload content of this hash table entry. */
  template <typename T>
  T *PayloadAs() noexcept {
    return reinterpret_cast<T *>(payload_);
  }

  /** @return A const-view typed pointer to the payload content of this hash table entry. */
  template <typename T>
  const T *PayloadAs() const noexcept {
    return reinterpret_cast<const T *>(payload_);
  }
};

/**
 * An iterator over a chain of hash table entries that match a provided initial hash value.
 * This iterator cannot resolve hash collisions, it is the responsibility of the user to do so.
 * Use as follows:
 *
 * @code
 * for (auto iter = jht.Lookup(hash); iter.HasNext();) {
 *   if (auto payload = (Payload*)iter.GetMatchPayload(); payload->key == my_key) {
 *     // Match
 *   }
 * }
 * @endcode
 *
 * @em NextMatch() must be called per iteration.
 */
struct HashTableEntryIterator {
 public:
  /**
   * Construct an iterator over hash table entry's beginning at the provided initial entry that
   * match the provided hash value. It is the responsibility of the user to resolve hash collisions.
   * This iterator is returned from JoinHashTable::Lookup().
   * @param initial The first matching entry in the chain of entries
   * @param hash The hash value of the probe tuple
   */
  HashTableEntryIterator(const HashTableEntry *initial, hash_t hash) : next_(initial), hash_(hash) {}

  /**
   * Advance to the next match and return true if it is found.
   * @return True if there is at least one more potential match.
   */
  bool HasNext() {
    while (next_ != nullptr) {
      if (next_->hash_ == hash_) {
        return true;
      }
      next_ = next_->next_;
    }
    return false;
  }

  /** @return The next match. */
  const HashTableEntry *GetMatch() {
    const HashTableEntry *result = next_;
    next_ = next_->next_;
    return result;
  }

  /** @return The payload of the next matched entry. */
  const byte *GetMatchPayload() { return GetMatch()->PayloadAs<byte>(); }

 private:
  // The next element the iterator produces.
  const HashTableEntry *next_;

  // The hash value we're looking up. Used as a cheap pre-filter in key-equality checks.
  hash_t hash_;
};

}  // namespace noisepage::execution::sql
