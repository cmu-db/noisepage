#include <unordered_map>

#include "common/math_util.h"
#include "execution/sql/generic_hash_table.h"

namespace terrier::execution::sql {

GenericHashTable::GenericHashTable(float load_factor) noexcept : load_factor_(load_factor) {}

GenericHashTable::~GenericHashTable() {
  if (entries_ != nullptr) {
    // Called from tearDown(), so don't pass a MemoryTracker
    util::TrackFreeHugeArray(nullptr, entries_, Capacity());
  }
}

void GenericHashTable::SetSize(uint64_t new_size, common::ManagedPointer<MemoryTracker> tracker) {
  TERRIER_ASSERT(new_size > 0, "New size cannot be zero!");
  if (entries_ != nullptr) {
    util::TrackFreeHugeArray(tracker, entries_, Capacity());
  }

  auto next_size = static_cast<double>(common::MathUtil::PowerOf2Ceil(new_size));
  if (next_size < (static_cast<double>(new_size) / load_factor_)) {
    next_size *= 2;
  }

  capacity_ = static_cast<uint64_t>(next_size);
  mask_ = capacity_ - 1;
  num_elems_ = 0;
  entries_ = util::TrackMallocHugeArray<std::atomic<HashTableEntry *>>(tracker, capacity_);
}

}  // namespace terrier::execution::sql
