#include "execution/sql/concise_hash_table.h"

#include <algorithm>

#include "execution/util/bit_util.h"

namespace terrier::execution::sql {

ConciseHashTable::ConciseHashTable(const uint32_t probe_threshold)
    : slot_groups_(nullptr),
      num_groups_(0),
      slot_mask_(0),
      probe_limit_(probe_threshold),
      num_overflow_(0),
      built_(false) {}

ConciseHashTable::~ConciseHashTable() {
  if (slot_groups_ != nullptr) {
    util::Memory::FreeHugeArray(slot_groups_, num_groups_);
  }
}

void ConciseHashTable::SetSize(const uint32_t new_size) {
  if (slot_groups_ != nullptr) {
    util::Memory::FreeHugeArray(slot_groups_, num_groups_);
  }

  uint64_t capacity = std::max(MIN_NUM_SLOTS, common::MathUtil::PowerOf2Floor(new_size * LOAD_FACTOR));
  slot_mask_ = capacity - 1;
  num_groups_ = capacity >> LOG_SLOTS_PER_GROUP;
  slot_groups_ = util::Memory::MallocHugeArray<SlotGroup>(num_groups_, true);
}

void ConciseHashTable::Build() {
  if (IsBuilt()) {
    return;
  }

  // Compute the prefix counts for each slot group

  slot_groups_[0].count = util::BitUtil::CountPopulation(slot_groups_[0].bits);

  for (uint64_t i = 1; i < num_groups_; i++) {
    slot_groups_[i].count = slot_groups_[i - 1].count + util::BitUtil::CountPopulation(slot_groups_[i].bits);
  }

  built_ = true;
}

}  // namespace terrier::execution::sql
