#include "execution/sql/aggregation_hash_table.h"

namespace tpl::sql {

AggregationHashTable::AggregationHashTable(util::Region *region,
                                           u32 tuple_size) noexcept
    : entries_(region, tuple_size),
      max_fill_(static_cast<u64>(static_cast<float>(kDefaultInitialTableSize) * 0.7f)) {
  hash_table_.SetSize(kDefaultInitialTableSize);
}

void AggregationHashTable::Grow() {
  // Resize table
  auto new_size = hash_table_.capacity() * 2;
  max_fill_ = static_cast<u64>(static_cast<float>(new_size) * 0.7f);
  hash_table_.SetSize(new_size);

  // Insert elements again
  for (auto *untyped_entry : entries_) {
    auto *entry = reinterpret_cast<HashTableEntry *>(untyped_entry);
    hash_table_.Insert<false>(entry, entry->hash);
  }
}

byte *AggregationHashTable::Insert(hash_t hash) noexcept {
  // Grow if need be
  if (NeedsToGrow()) {
    Grow();
  }

  // Allocate an entry
  auto *entry = reinterpret_cast<HashTableEntry *>(entries_.append());
  entry->hash = hash;
  entry->next = nullptr;

  // Insert into table
  hash_table_.Insert<false>(entry, entry->hash);

  // Give the payload so the client can write into it
  return entry->payload;
}

}  // namespace tpl::sql
