#include <algorithm>

#include "execution/sql/concise_hash_table.h"
#include "execution/util/bit_util.h"

namespace terrier::execution::sql {

ConciseHashTable::ConciseHashTable(uint32_t probe_threshold) : probe_limit_(probe_threshold) {}

ConciseHashTable::~ConciseHashTable() {
  if (slot_groups_ != nullptr) {
    util::FreeHugeArray(slot_groups_, num_groups_);
  }
}

void ConciseHashTable::SetSize(const uint32_t num_elems) {
  if (slot_groups_ != nullptr) {
    util::FreeHugeArray(slot_groups_, num_groups_);
  }

  uint64_t capacity = std::max(K_MIN_NUM_SLOTS, common::MathUtil::PowerOf2Floor(num_elems * K_LOAD_FACTOR));
  slot_mask_ = capacity - 1;
  num_groups_ = capacity >> K_LOG_SLOTS_PER_GROUP;
  slot_groups_ = util::MallocHugeArray<SlotGroup>(num_groups_);
}

void ConciseHashTable::Build() {
  if (IsBuilt()) {
    return;
  }

  // Compute the prefix counts for each slot group

  slot_groups_[0].count_ = util::BitUtil::CountBits(slot_groups_[0].bits_);

  for (uint32_t i = 1; i < num_groups_; i++) {
    slot_groups_[i].count_ = slot_groups_[i - 1].count_ + util::BitUtil::CountBits(slot_groups_[i].bits_);
  }

  built_ = true;
}

}  // namespace terrier::execution::sql
