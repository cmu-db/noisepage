#pragma once

#include <algorithm>
#include <memory>
#include <utility>

#include "execution/sql/hash_table_entry.h"
#include "execution/util/bit_util.h"
#include "execution/util/common.h"
#include "execution/util/memory.h"

namespace tpl::sql {

class ConciseHashTable {
 public:
  // The maximum probe length before falling back into the overflow table
  static constexpr const u32 kProbeThreshold = 1;

  // The default load factor
  static constexpr const u32 kLoadFactor = 8;

  // A minimum of 4K slots
  static constexpr const u64 kMinNumSlots = 1u << 12;

  // The number of CHT slots that belong to one group. This value should either
  // be 32 or 64 for (1) making computation simpler by bit-shifting and (2) to
  // ensure at most one cache-line read/write per insert/lookup.
  static constexpr const u32 kLogSlotsPerGroup = 6;
  static constexpr const u32 kSlotsPerGroup = 1u << kLogSlotsPerGroup;
  static constexpr const u32 kGroupBitMask = kSlotsPerGroup - 1;

  /// Create a new uninitialized concise hash table. Callers **must** call
  /// SetSize() before interacting with the table
  explicit ConciseHashTable(u32 probe_threshold = kProbeThreshold);

  /// Destroy
  ~ConciseHashTable();

  /// This class cannot be copied or moved
  DISALLOW_COPY_AND_MOVE(ConciseHashTable)

  /// Set the size of the hash table to support at least \a num_elems elements
  void SetSize(u32 num_elems);

  /// Insert an element with the given hash into the table and return an encoded
  /// slot position
  void Insert(HashTableEntry *entry, hash_t hash);

  /// Finalize and build this concise hash table
  void Build();

  /// Prefetch the slot group for the given slot \a slot
  template <bool ForRead>
  void PrefetchSlotGroup(hash_t hash) const;

  /// Return the number of occupied slots in the table **before** the given slot
  u64 NumFilledSlotsBefore(ConciseHashTableSlot slot) const;

  /// Lookup the slot for the given hash
  std::pair<bool, u64> Lookup(hash_t hash) const;

  // -------------------------------------------------------
  // Utility Operations
  // -------------------------------------------------------

  /// Return the number of bytes this hash table has allocated
  u64 GetTotalMemoryUsage() const { return sizeof(SlotGroup) * num_groups_; }

  // -------------------------------------------------------
  // Accessors
  // -------------------------------------------------------

  /// Return the capacity (the maximum number of elements) this table supports
  u64 capacity() const { return slot_mask_ + 1; }

  /// Return the number of overflows entries in this table
  u64 num_overflow() const { return num_overflow_; }

  /// Has the table been built?
  bool is_built() const { return built_; }

 private:
  /// A slot group represents a group of 64 slots. Each slot is represented as a
  /// single bit from the \a bits field. \a count is a count of the number of
  /// set bits in all slot groups in the group array up to and including this
  /// group. In other worse, \a count is a prefix count of the number of filled
  /// slots up to this group.
  struct SlotGroup {
    // The bitmap indicating whether the slots are occupied or free
    u64 bits;
    // The prefix population count
    u32 count;

    static_assert(sizeof(bits) * kBitsPerByte == kSlotsPerGroup,
                  "Number of slots in group and configured constant are out of sync");
  } PACKED;

 private:
  // The array of groups. This array is managed by this class.
  SlotGroup *slot_groups_;

  // The number of groups (of slots) in the table
  u64 num_groups_;

  // The mask used to find a slot in the hash table
  u64 slot_mask_;

  // The maximum number of slots to probe
  u32 probe_limit_;

  // The number of entries in the overflow table
  u32 num_overflow_;

  // Flag indicating if the hash table has been built and is frozen (read-only)
  bool built_;
};

// ---------------------------------------------------------
// Implementation below
// ---------------------------------------------------------

inline void ConciseHashTable::Insert(HashTableEntry *entry, const hash_t hash) {
  const u64 slot_idx = hash & slot_mask_;
  const u64 group_idx = slot_idx >> kLogSlotsPerGroup;
  const u64 num_bits_to_group = group_idx << kLogSlotsPerGroup;
  u32 *group_bits = reinterpret_cast<u32 *>(&slot_groups_[group_idx].bits);

  u32 bit_idx = static_cast<u32>(slot_idx & kGroupBitMask);
  u32 max_bit_idx = std::min(63u, bit_idx + probe_limit_);
  do {
    if (!util::BitUtil::Test(group_bits, bit_idx)) {
      util::BitUtil::Set(group_bits, bit_idx);
      entry->cht_slot = ConciseHashTableSlot(num_bits_to_group + bit_idx);
      return;
    }
  } while (++bit_idx <= max_bit_idx);

  num_overflow_++;

  entry->cht_slot = ConciseHashTableSlot(num_bits_to_group + bit_idx - 1);
}

template <bool ForRead>
inline void ConciseHashTable::PrefetchSlotGroup(hash_t hash) const {
  const u64 slot_idx = hash & slot_mask_;
  const u64 group_idx = slot_idx >> kLogSlotsPerGroup;
  util::Prefetch<ForRead, Locality::Low>(slot_groups_ + group_idx);
}

inline u64 ConciseHashTable::NumFilledSlotsBefore(const ConciseHashTableSlot slot) const {
  TPL_ASSERT(is_built(), "Table must be built");

  const u64 group_idx = slot >> kLogSlotsPerGroup;
  const u64 bit_idx = slot & kGroupBitMask;

  const SlotGroup *slot_group = slot_groups_ + group_idx;
  const u64 bits_after_slot = slot_group->bits & (u64(-1) << bit_idx);
  return slot_group->count - util::BitUtil::CountBits(bits_after_slot);
}

inline std::pair<bool, u64> ConciseHashTable::Lookup(const hash_t hash) const {
  const u64 slot_idx = hash & slot_mask_;
  const u64 group_idx = slot_idx >> kLogSlotsPerGroup;
  const u64 bit_idx = slot_idx & kGroupBitMask;

  const SlotGroup *slot_group = slot_groups_ + group_idx;
  const u64 bits_after_slot = slot_group->bits & (u64(-1) << bit_idx);

  const bool exists = slot_group->bits & (1ull << bit_idx);
  const u64 pos = slot_group->count - util::BitUtil::CountBits(bits_after_slot);

  return std::pair(exists, pos);
}

}  // namespace tpl::sql
