#include "execution/sql/join_hash_table.h"

namespace noisepage::execution::sql {

JoinHashTableIterator::JoinHashTableIterator(const JoinHashTable &table)
    : entry_list_iter_(table.owned_.begin()),
      entry_list_end_(table.owned_.end()),
      entry_iter_(table.entries_.begin()),
      entry_end_(table.entries_.end()) {
  NOISEPAGE_ASSERT(table.IsBuilt(), "Cannot iterate over a JoinHashTable that hasn't been built yet!");
  if (!table.owned_.empty()) FindNextNonEmptyList();
}

void JoinHashTableIterator::FindNextNonEmptyList() {
  for (; entry_list_iter_ != entry_list_end_ && entry_iter_ == entry_end_; ++entry_list_iter_) {
    entry_iter_ = entry_list_iter_->begin();
    entry_end_ = entry_list_iter_->end();
  }
}

}  // namespace noisepage::execution::sql
