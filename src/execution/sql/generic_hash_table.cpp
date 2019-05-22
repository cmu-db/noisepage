#include "execution/sql/generic_hash_table.h"

#include "execution/util/math_util.h"

namespace tpl::sql {

GenericHashTable::GenericHashTable(float load_factor) noexcept
    : entries_(nullptr), mask_(0), capacity_(0), num_elems_(0), load_factor_(load_factor) {}

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

  u64 next_size = util::MathUtil::PowerOf2Ceil(new_size);
  if (static_cast<double>(next_size) < (static_cast<double>(new_size) / load_factor_)) {
    next_size *= 2;
  }

  capacity_ = next_size;
  mask_ = capacity_ - 1;
  entries_ = util::MallocHugeArray<std::atomic<HashTableEntry *>>(capacity_);
}

}  // namespace tpl::sql
