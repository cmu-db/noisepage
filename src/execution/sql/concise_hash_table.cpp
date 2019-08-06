#include <algorithm>

#include "execution/sql/concise_hash_table.h"
#include "execution/util/bit_util.h"

namespace terrier::sql {

ConciseHashTable::ConciseHashTable(u32 probe_threshold) : probe_limit_(probe_threshold) {}

ConciseHashTable::~ConciseHashTable() {
  if (slot_groups_ != nullptr) {
    util::FreeHugeArray(slot_groups_, num_groups_);
  }
}

void ConciseHashTable::SetSize(const u32 num_elems) {
  if (slot_groups_ != nullptr) {
    util::FreeHugeArray(slot_groups_, num_groups_);
  }

  u64 capacity = std::max(kMinNumSlots, util::MathUtil::PowerOf2Floor(num_elems * kLoadFactor));
  slot_mask_ = capacity - 1;
  num_groups_ = capacity >> kLogSlotsPerGroup;
  slot_groups_ = util::MallocHugeArray<SlotGroup>(num_groups_);
}

void ConciseHashTable::Build() {
  if (is_built()) {
    return;
  }

  // Compute the prefix counts for each slot group

  slot_groups_[0].count = util::BitUtil::CountBits(slot_groups_[0].bits);

  for (u32 i = 1; i < num_groups_; i++) {
    slot_groups_[i].count = slot_groups_[i - 1].count + util::BitUtil::CountBits(slot_groups_[i].bits);
  }

  built_ = true;
}

}  // namespace terrier::sql
