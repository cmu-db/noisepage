#include "execution/sql/aggregation_hash_table.h"

#include <algorithm>

#include "execution/sql/projected_columns_iterator.h"
#include "execution/util/bit_util.h"
#include "execution/util/cpu_info.h"
#include "execution/util/math_util.h"
#include "execution/util/vector_util.h"
#include "loggers/execution_logger.h"

namespace tpl::sql {

AggregationHashTable::AggregationHashTable(MemoryPool *memory, u32 payload_size)
    : memory_(memory),
      entries_(sizeof(HashTableEntry) + payload_size, MemoryPoolAllocator<byte>(memory)),
      hash_table_(kDefaultLoadFactor),
      partition_heads_(nullptr),
      partition_tails_(nullptr),
      partition_tables_(nullptr),
      part_shift_bits_(util::BitUtil::CountLeadingZeros(u64(kDefaultNumPartitions) - 1)) {
  // Set the table to a decent initial size and the max fill to determine when
  // to resize the table next
  hash_table_.SetSize(kDefaultInitialTableSize);
  max_fill_ = static_cast<u64>(std::llround(f32(hash_table_.capacity()) * hash_table_.load_factor()));

  // Compute flush threshold. In partitioned mode, we want the thread-local
  // pre-aggregation hash table to be sized to fit in cache. Target L2.
  const u64 l2_size = CpuInfo::Instance()->GetCacheSize(CpuInfo::L2_CACHE);
  flush_threshold_ = static_cast<u64>(std::llround(f32(l2_size) / f32(entries_.element_size()) * kDefaultLoadFactor));
  flush_threshold_ = std::max(256ul, util::MathUtil::PowerOf2Floor(flush_threshold_));
}

AggregationHashTable::~AggregationHashTable() {
  if (partition_heads_ != nullptr) {
    const auto num_bytes = kDefaultNumPartitions * sizeof(HashTableEntry *);
    memory_->Deallocate(partition_heads_, num_bytes);
  }
  if (partition_tails_ != nullptr) {
    const auto num_bytes = kDefaultNumPartitions * sizeof(HashTableEntry *);
    memory_->Deallocate(partition_tails_, num_bytes);
  }
  if (partition_tables_ != nullptr) {
    const auto num_bytes = kDefaultNumPartitions * sizeof(AggregationHashTable *);
    memory_->Deallocate(partition_tables_, num_bytes);
  }
}

void AggregationHashTable::Grow() {
  // Resize table
  const u64 new_size = hash_table_.capacity() * 2;
  hash_table_.SetSize(new_size);
  max_fill_ = static_cast<u64>(std::llround(f32(hash_table_.capacity()) * hash_table_.load_factor()));

  // Insert elements again
  for (byte *untyped_entry : entries_) {
    auto *entry = reinterpret_cast<HashTableEntry *>(untyped_entry);
    hash_table_.Insert<false>(entry, entry->hash);
  }

  // Update stats
  stats_.num_growths++;
}

byte *AggregationHashTable::Insert(const hash_t hash) {
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

byte *AggregationHashTable::InsertPartitioned(const hash_t hash) {
  byte *ret = Insert(hash);
  if (hash_table_.num_elements() >= flush_threshold_) {
    FlushToOverflowPartitions();
  }
  return ret;
}

void AggregationHashTable::FlushToOverflowPartitions() {
  if (TPL_UNLIKELY(partition_heads_ == nullptr)) {
    TPL_ASSERT(partition_tails_ == nullptr, "Partition tail isn't null when head is null");
    partition_heads_ = memory_->AllocateArray<HashTableEntry *>(kDefaultNumPartitions, true);
    partition_tails_ = memory_->AllocateArray<HashTableEntry *>(kDefaultNumPartitions, true);
  }

  // Dump hash table into overflow partition
  hash_table_.FlushEntries([this](HashTableEntry *entry) {
    const u64 part_idx = (entry->hash >> part_shift_bits_);
    entry->next = partition_heads_[part_idx];
    partition_heads_[part_idx] = entry;
    if (TPL_UNLIKELY(partition_tails_[part_idx] == nullptr)) {
      partition_tails_[part_idx] = entry;
    }
  });

  // Update stats
  stats_.num_flushes++;
}

void AggregationHashTable::ProcessBatch(ProjectedColumnsIterator *iters[], AggregationHashTable::HashFn hash_fn,
                                        KeyEqFn key_eq_fn, AggregationHashTable::InitAggFn init_agg_fn,
                                        AggregationHashTable::AdvanceAggFn advance_agg_fn) {
  TPL_ASSERT(iters != nullptr, "Null input iterators!");
  const u32 num_elems = iters[0]->num_selected();

  // Temporary vector for the hash values and hash table entry pointers
  alignas(CACHELINE_SIZE) hash_t hashes[kDefaultVectorSize];
  alignas(CACHELINE_SIZE) HashTableEntry *entries[kDefaultVectorSize];

  if (iters[0]->IsFiltered()) {
    ProcessBatchImpl<true>(iters, num_elems, hashes, entries, hash_fn, key_eq_fn, init_agg_fn, advance_agg_fn);
  } else {
    ProcessBatchImpl<false>(iters, num_elems, hashes, entries, hash_fn, key_eq_fn, init_agg_fn, advance_agg_fn);
  }
}

template <bool PCIIsFiltered>
void AggregationHashTable::ProcessBatchImpl(ProjectedColumnsIterator *iters[], u32 num_elems, hash_t hashes[],
                                            HashTableEntry *entries[], AggregationHashTable::HashFn hash_fn,
                                            KeyEqFn key_eq_fn, AggregationHashTable::InitAggFn init_agg_fn,
                                            AggregationHashTable::AdvanceAggFn advance_agg_fn) {
  // Lookup batch
  LookupBatch<PCIIsFiltered>(iters, num_elems, hashes, entries, hash_fn, key_eq_fn);
  iters[0]->Reset();

  // Create missing groups
  CreateMissingGroups<PCIIsFiltered>(iters, num_elems, hashes, entries, key_eq_fn, init_agg_fn);
  iters[0]->Reset();

  // Update valid groups
  AdvanceGroups<PCIIsFiltered>(iters, num_elems, entries, advance_agg_fn);
  iters[0]->Reset();
}

template <bool PCIIsFiltered>
void AggregationHashTable::LookupBatch(ProjectedColumnsIterator *iters[], u32 num_elems, hash_t hashes[],
                                       HashTableEntry *entries[], AggregationHashTable::HashFn hash_fn,
                                       AggregationHashTable::KeyEqFn key_eq_fn) const {
  // Compute hash and perform initial lookup
  ComputeHashAndLoadInitial<PCIIsFiltered>(iters, num_elems, hashes, entries, hash_fn);

  // Determine the indexes of entries that are non-null
  alignas(CACHELINE_SIZE) u32 group_sel[kDefaultVectorSize];
  u32 num_groups = util::VectorUtil::FilterNe(reinterpret_cast<intptr_t *>(entries), iters[0]->num_selected(),
                                              intptr_t(0), group_sel, nullptr);

  // Candidate groups in 'entries' may have hash collisions. Follow the chain
  // to check key equality.
  FollowNextLoop<PCIIsFiltered>(iters, num_groups, group_sel, hashes, entries, key_eq_fn);
}

template <bool PCIIsFiltered>
void AggregationHashTable::ComputeHashAndLoadInitial(ProjectedColumnsIterator *iters[], u32 num_elems, hash_t hashes[],
                                                     HashTableEntry *entries[],
                                                     AggregationHashTable::HashFn hash_fn) const {
  // If the hash table is larger than cache, inject prefetch instructions
  u64 l3_cache_size = CpuInfo::Instance()->GetCacheSize(CpuInfo::L3_CACHE);
  if (hash_table_.GetTotalMemoryUsage() > l3_cache_size) {
    ComputeHashAndLoadInitialImpl<PCIIsFiltered, true>(iters, num_elems, hashes, entries, hash_fn);
  } else {
    ComputeHashAndLoadInitialImpl<PCIIsFiltered, false>(iters, num_elems, hashes, entries, hash_fn);
  }
}

template <bool PCIIsFiltered, bool Prefetch>
void AggregationHashTable::ComputeHashAndLoadInitialImpl(ProjectedColumnsIterator *iters[], u32 num_elems,
                                                         hash_t hashes[], HashTableEntry *entries[],
                                                         AggregationHashTable::HashFn hash_fn) const {
  // Compute hash
  if constexpr (PCIIsFiltered) {
    for (u32 idx = 0; iters[0]->HasNextFiltered(); iters[0]->AdvanceFiltered()) {
      hashes[idx++] = hash_fn(iters);
    }
  } else {  // NOLINT
    for (u32 idx = 0; iters[0]->HasNext(); iters[0]->Advance()) {
      hashes[idx++] = hash_fn(iters);
    }
  }

  // Reset PCI
  iters[0]->Reset();

  // Load entries
  for (u32 idx = 0, prefetch_idx = kPrefetchDistance; idx < num_elems; idx++, prefetch_idx++) {
    if constexpr (Prefetch) {
      if (TPL_LIKELY(prefetch_idx < num_elems)) {
        // NOLINTNEXTLINE
        hash_table_.PrefetchChainHead<false>(hashes[prefetch_idx]);
      }
    }
    // Load chain head
    // NOLINTNEXTLINE
    entries[idx] = hash_table_.FindChainHead(hashes[idx]);
  }
}

template <bool PCIIsFiltered>
void AggregationHashTable::FollowNextLoop(ProjectedColumnsIterator *iters[], u32 num_elems, u32 group_sel[],
                                          const hash_t hashes[], HashTableEntry *entries[],
                                          AggregationHashTable::KeyEqFn key_eq_fn) const {
  while (num_elems > 0) {
    u32 write_idx = 0;

    // Simultaneously iterate over valid groups and input probe tuples in the
    // vector projection and check key equality for each. For mismatches,
    // follow the bucket chain.
    for (u32 idx = 0; idx < num_elems; idx++) {
      iters[0]->SetPosition<PCIIsFiltered>(group_sel[idx]);

      const bool keys_match =
          entries[group_sel[idx]]->hash == hashes[group_sel[idx]] && key_eq_fn(entries[group_sel[idx]]->payload, iters);
      const bool has_next = entries[group_sel[idx]]->next != nullptr;

      group_sel[write_idx] = group_sel[idx];
      write_idx += static_cast<u32>(!keys_match && has_next);
    }

    // Reset PCI
    iters[0]->Reset();

    // Follow chain
    for (u32 idx = 0; idx < write_idx; idx++) {
      HashTableEntry *&entry = entries[group_sel[idx]];
      entry = entry->next;
    }

    // Next
    num_elems = write_idx;
  }
}

template <bool PCIIsFiltered>
void AggregationHashTable::CreateMissingGroups(ProjectedColumnsIterator *iters[], u32 num_elems, const hash_t hashes[],
                                               HashTableEntry *entries[], AggregationHashTable::KeyEqFn key_eq_fn,
                                               AggregationHashTable::InitAggFn init_agg_fn) {
  // Vector storing all the missing group IDs
  alignas(CACHELINE_SIZE) u32 group_sel[kDefaultVectorSize];

  // Determine which elements are missing a group
  u32 num_groups =
      util::VectorUtil::FilterEq(reinterpret_cast<intptr_t *>(entries), num_elems, intptr_t(0), group_sel, nullptr);

  // Insert those elements
  for (u32 idx = 0; idx < num_groups; idx++) {
    hash_t hash = hashes[group_sel[idx]];

    if (HashTableEntry *entry = LookupEntryInternal(hash, key_eq_fn, iters); entry != nullptr) {
      entries[group_sel[idx]] = entry;
      continue;
    }

    // Move PCI to position of new aggregate
    iters[0]->SetPosition<PCIIsFiltered>(group_sel[idx]);

    // Initialize
    init_agg_fn(Insert(hash), iters);
  }
}

template <bool PCIIsFiltered>
void AggregationHashTable::AdvanceGroups(ProjectedColumnsIterator *iters[], u32 num_elems, HashTableEntry *entries[],
                                         AggregationHashTable::AdvanceAggFn advance_agg_fn) {
  // Vector storing all valid group indexes
  alignas(CACHELINE_SIZE) u32 group_sel[kDefaultVectorSize];

  // All non-null entries are groups that should be updated. Find them now.
  u32 num_groups =
      util::VectorUtil::FilterNe(reinterpret_cast<intptr_t *>(entries), num_elems, intptr_t(0), group_sel, nullptr);

  // Group indexes are stored in group_sel, update them now.
  for (u32 idx = 0; idx < num_groups; idx++) {
    HashTableEntry *entry = entries[group_sel[idx]];
    iters[0]->SetPosition<PCIIsFiltered>(group_sel[idx]);
    advance_agg_fn(entry->payload, iters);
  }
}

}  // namespace tpl::sql
