#include <unordered_map>

#include "execution/sql/generic_hash_table.h"
#include "execution/util/math_util.h"

namespace terrier::execution::sql {

GenericHashTable::GenericHashTable(float load_factor) noexcept : load_factor_(load_factor) {}

GenericHashTable::~GenericHashTable() {
  if (entries_ != nullptr) {
    util::FreeHugeArray(entries_, capacity());
  }
}

void GenericHashTable::SetSize(u64 new_size) {
  TPL_ASSERT(new_size > 0, "New size cannot be zero!");
  if (entries_ != nullptr) {
    util::FreeHugeArray(entries_, capacity());
  }

  auto next_size = static_cast<double>(util::MathUtil::PowerOf2Ceil(new_size));
  if (next_size < (static_cast<double>(new_size) / load_factor_)) {
    next_size *= 2;
  }

  capacity_ = static_cast<u64>(next_size);
  mask_ = capacity_ - 1;
  num_elems_ = 0;
  entries_ = util::MallocHugeArray<std::atomic<HashTableEntry *>>(capacity_);
}

}  // namespace terrier::execution::sql
