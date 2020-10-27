#include "execution/sql/concise_hash_table.h"

#include <algorithm>

#include "execution/util/bit_util.h"

namespace noisepage::execution::sql {

ConciseHashTable::ConciseHashTable(const uint32_t probe_threshold) : probe_limit_(probe_threshold) {}

ConciseHashTable::~ConciseHashTable() {
  if (slot_groups_ != nullptr) {
    util::Memory::TrackFreeHugeArray(nullptr, slot_groups_, num_groups_);
  }
}

void ConciseHashTable::SetSize(const uint32_t new_size, common::ManagedPointer<MemoryTracker> tracker) {
  if (slot_groups_ != nullptr) {
    util::Memory::TrackFreeHugeArray(tracker, slot_groups_, num_groups_);
  }

  uint64_t capacity = std::max(MIN_NUM_SLOTS, common::MathUtil::PowerOf2Floor(new_size * LOAD_FACTOR));
  slot_mask_ = capacity - 1;
  num_groups_ = capacity >> LOG_SLOTS_PER_GROUP;
  slot_groups_ = util::Memory::TrackMallocHugeArray<SlotGroup>(tracker, num_groups_, true);
}

void ConciseHashTable::Build() {
  if (IsBuilt()) {
    return;
  }

  // Compute the prefix counts for each slot group

  slot_groups_[0].count_ = util::BitUtil::CountPopulation(slot_groups_[0].bits_);

  for (uint64_t i = 1; i < num_groups_; i++) {
    slot_groups_[i].count_ = slot_groups_[i - 1].count_ + util::BitUtil::CountPopulation(slot_groups_[i].bits_);
  }

  built_ = true;
}

}  // namespace noisepage::execution::sql
