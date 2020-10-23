#pragma once

#include <algorithm>
#include <memory>
#include <utility>

#include "common/math_util.h"
#include "execution/sql/hash_table_entry.h"
#include "execution/util/bit_util.h"
#include "execution/util/chunked_vector.h"
#include "execution/util/cpu_info.h"
#include "execution/util/memory.h"

namespace noisepage::execution::sql {

/**
 * A concise hash table that uses a bitmap to determine slot occupation. A concise hash table is
 * meant to be bulk loaded through calls to ConciseHashTable::Insert() and frozen through
 * ConciseHashTable::Build(). Thus, it is a write-only-read-many (WORM) data structure.
 *
 * The (potential) advantage over vanilla hash tables is the space savings since only one bit
 * is needed per-entry ([1]). This offers up to an 8x space savings, depending on the chosen probing
 * limit. These space savings encourage in-cache processing which can leverage SIMD vectorized
 * instructions for even faster join processing throughput.
 *
 * [1]: In actuality, the overhead per element is larger than one bit.
 */
class ConciseHashTable {
 public:
  /** The maximum probe length before falling back into the overflow table. */
  static constexpr const uint64_t PROBE_THRESHOLD = 1;

  /** The default load factor. */
  static constexpr const uint64_t LOAD_FACTOR = 8;

  /** A minimum of 4K slots. */
  static constexpr const uint64_t MIN_NUM_SLOTS = 1u << 12u;

  // The number of CHT slots that belong to one group.
  // This value should either be 32 or 64 for
  // (1) making computation simpler by bit-shifting, and
  // (2) to ensure at most one cache-line read/write per insert/lookup.
  /** Log of the number of slots per group. */
  static constexpr const uint64_t LOG_SLOTS_PER_GROUP = 5;
  /** Number of slots per group. */
  static constexpr const uint64_t SLOTS_PER_GROUP = 1u << LOG_SLOTS_PER_GROUP;
  /** Log of the number of slots per group. */
  static constexpr const uint64_t GROUP_BIT_MASK = SLOTS_PER_GROUP - 1;

  // Sanity check
  static_assert(common::MathUtil::IsPowerOf2(MIN_NUM_SLOTS), "Minimum slot count must be a power of 2");

  /**
   * Create a new uninitialized concise hash table. Callers must call @em SetSize() before
   * inserting into the table.
   * @param probe_threshold The maximum probe threshold before falling back to a secondary overflow
   *                        entry table.
   */
  explicit ConciseHashTable(uint32_t probe_threshold = PROBE_THRESHOLD);

  /**
   * Destructor.
   */
  ~ConciseHashTable();

  /**
   * This class cannot be copied or moved.
   */
  DISALLOW_COPY_AND_MOVE(ConciseHashTable)

  /**
   * Set the size of the hash table to support at least @em num_elems entries. The table will
   * optimize itself in expectation of seeing at most @em num_elems elements without resizing.
   * @param new_size The expected number of elements.
   * @param tracker MemoryTracker
   */
  void SetSize(uint32_t new_size, common::ManagedPointer<MemoryTracker> tracker);

  /**
   * Insert an element into the table. After insertion, the input entry's CHT slot member will be
   * populated with the slot in this hash table it resides.
   * @param entry The entry to insert
   */
  void Insert(HashTableEntry *entry);

  /**
   * Insert a list of entries into this hash table.
   * @tparam Allocator The allocator used by the vector.
   * @param entries The list of entries to insert into the table.
   */
  template <typename Allocator>
  void InsertBatch(util::ChunkedVector<Allocator> *entries);

  /**
   * Finalize and build this concise hash table. The table is frozen after finalization.
   */
  void Build();

  /**
   * Prefetch the slot group for an entry with the given hash value @em hash.
   * @tparam ForRead Is the prefetch for a subsequent read operation.
   * @param hash The hash value of the entry to prefetch.
   */
  template <bool ForRead>
  void PrefetchSlotGroup(hash_t hash) const;

  /**
   * Return the number of occupied slots in the table **before** the given slot.
   * @param slot The slot to compute the prefix count for.
   * @return The number of occupied slot before the provided input slot.
   */
  uint64_t NumFilledSlotsBefore(ConciseHashTableSlot slot) const;

  /**
   * Given the probe entry's hash value, return a boolean indicating whether it
   * may exist in the table, and if so, the index of the first slot the entry
   * may be.
   * @param hash The hash value of the entry to lookup
   * @return A pair indicating if the entry may exist and the slot to look at
   */
  std::pair<bool, uint64_t> Lookup(hash_t hash) const;

  /**
   * @return The number of bytes this hash table has allocated.
   */
  uint64_t GetTotalMemoryUsage() const { return sizeof(SlotGroup) * num_groups_; }

  /**
   * @return The capacity, i.e., the maximum number of elements this table supports.
   */
  uint64_t GetCapacity() const { return slot_mask_ + 1; }

  /**
   * @return The number of overflows entries in this table.
   */
  uint64_t GetOverflowEntryCount() const { return num_overflow_; }

  /**
   * @return True if the table has been built; false otherwise.
   */
  bool IsBuilt() const { return built_; }

 private:
  /**
   * A slot group represents a group of 64 slots. Each slot is represented as a single bit from the
   * @em bits field. @em count is a count of occupied slots in all slot groups up to and including
   * this group, or in other words, a prefix count of the number of filled slots up to this group.
   */
  struct SlotGroup {
    // The bitmap indicating whether the slots are occupied or free
    uint32_t bits_;
    // The prefix population count
    uint32_t count_;

    static_assert(sizeof(bits_) * common::Constants::K_BITS_PER_BYTE == SLOTS_PER_GROUP,
                  "# of slots in group and configured constant are out of sync!");
  };

  static_assert(sizeof(SlotGroup) == sizeof(uint64_t), "SlotGroup expected to be 64-bits");

  // Given a hash value, return its initial candidate slot index in the logical slot array
  uint64_t SlotIndex(hash_t hash) const noexcept { return hash & slot_mask_; }

  // Given a slot index, return the index of the group it falls to
  uint64_t GroupIndex(uint64_t slot_idx) const noexcept { return slot_idx >> LOG_SLOTS_PER_GROUP; }

  // Given a slot index, return the index of the bit in the group's slot chunk
  uint64_t GroupSlotIndex(uint64_t slot_index) const noexcept { return slot_index & GROUP_BIT_MASK; }

  // Insert a list of entries into the hash table.
  template <bool Prefetch, typename Allocator>
  void InsertBatchInternal(util::ChunkedVector<Allocator> *entries);

 private:
  // The array of groups. This array is managed by this class.
  SlotGroup *slot_groups_{nullptr};

  // The number of groups (of slots) in the table
  uint64_t num_groups_{0};

  // The mask used to find a slot in the hash table
  uint64_t slot_mask_{0};

  // The maximum number of slots to probe
  uint32_t probe_limit_;

  // The number of entries in the overflow table
  uint32_t num_overflow_{0};

  // Flag indicating if the hash table has been built and is frozen (read-only)
  bool built_{false};
};

// ---------------------------------------------------------
//
// Implementation below
//
// ---------------------------------------------------------

// The below methods are inlined in the header on purpose for performance. Please do not move them.

inline void ConciseHashTable::Insert(HashTableEntry *entry) {
  const uint64_t slot_idx = SlotIndex(entry->hash_);
  const uint64_t group_idx = GroupIndex(slot_idx);
  const uint64_t num_bits_to_group = group_idx * SLOTS_PER_GROUP;
  auto *group_bits = reinterpret_cast<uint32_t *>(&slot_groups_[group_idx].bits_);

  uint64_t bit_idx = static_cast<uint32_t>(GroupSlotIndex(slot_idx));
  const uint64_t max_bit_idx = std::min(SLOTS_PER_GROUP - 1, bit_idx + probe_limit_);
  do {
    if (!util::BitUtil::Test(group_bits, bit_idx)) {
      util::BitUtil::Set(group_bits, bit_idx);
      entry->cht_slot_ = ConciseHashTableSlot(num_bits_to_group + bit_idx);
      return;
    }
  } while (++bit_idx <= max_bit_idx);

  num_overflow_++;

  entry->cht_slot_ = ConciseHashTableSlot(num_bits_to_group + bit_idx - 1);
}

template <bool Prefetch, typename Allocator>
void ConciseHashTable::InsertBatchInternal(util::ChunkedVector<Allocator> *entries) {
  const uint64_t size = entries->size();
  for (uint64_t idx = 0, prefetch_idx = common::Constants::K_PREFETCH_DISTANCE; idx < size; idx++, prefetch_idx++) {
    if constexpr (Prefetch) {  // NOLINT
      if (LIKELY(prefetch_idx < size)) {
        auto *prefetch_entry = reinterpret_cast<HashTableEntry *>((*entries)[prefetch_idx]);
        PrefetchSlotGroup<false>(prefetch_entry->hash_);
      }
    }

    Insert(reinterpret_cast<HashTableEntry *>((*entries)[idx]));
  }
}

template <typename Allocator>
void ConciseHashTable::InsertBatch(util::ChunkedVector<Allocator> *entries) {
  uint64_t l3_cache_size = CpuInfo::Instance()->GetCacheSize(CpuInfo::L3_CACHE);
  if (const bool out_of_cache = GetTotalMemoryUsage() > l3_cache_size; out_of_cache) {
    InsertBatchInternal<true>(entries);
  } else {
    InsertBatchInternal<false>(entries);
  }
}

template <bool ForRead>
inline void ConciseHashTable::PrefetchSlotGroup(hash_t hash) const {
  const uint64_t slot_idx = SlotIndex(hash);
  const uint64_t group_idx = GroupIndex(slot_idx);
  util::Memory::Prefetch<ForRead, Locality::Low>(slot_groups_ + group_idx);
}

inline uint64_t ConciseHashTable::NumFilledSlotsBefore(const ConciseHashTableSlot slot) const {
  NOISEPAGE_ASSERT(IsBuilt(), "Table must be built");

  const uint64_t group_idx = GroupIndex(slot);
  const uint64_t bit_idx = GroupSlotIndex(slot);

  const SlotGroup *slot_group = slot_groups_ + group_idx;
  const uint64_t bits_after_slot = slot_group->bits_ & (uint64_t(-1) << bit_idx);
  return slot_group->count_ - util::BitUtil::CountPopulation(bits_after_slot);
}

inline std::pair<bool, uint64_t> ConciseHashTable::Lookup(const hash_t hash) const {
  const uint64_t slot_idx = SlotIndex(hash);
  const uint64_t group_idx = GroupIndex(slot_idx);
  const uint64_t bit_idx = GroupSlotIndex(slot_idx);

  const SlotGroup *slot_group = slot_groups_ + group_idx;
  const uint64_t bits_after_slot = slot_group->bits_ & (uint64_t(-1) << bit_idx);

  const bool exists = (slot_group->bits_ & (1ull << bit_idx)) != 0u;
  const uint64_t pos = slot_group->count_ - util::BitUtil::CountPopulation(bits_after_slot);

  return std::pair(exists, pos);
}

}  // namespace noisepage::execution::sql
