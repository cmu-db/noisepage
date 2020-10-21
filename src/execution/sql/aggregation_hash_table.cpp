#include "execution/sql/aggregation_hash_table.h"

#include <tbb/parallel_for_each.h>
#include <tbb/task_scheduler_init.h>

#include <algorithm>
#include <memory>
#include <numeric>
#include <utility>
#include <vector>

#include "common/error/exception.h"
#include "common/math_util.h"
#include "count/hll.h"
#include "execution/exec/execution_context.h"
#include "execution/sql/constant_vector.h"
#include "execution/sql/generic_value.h"
#include "execution/sql/thread_state_container.h"
#include "execution/sql/vector_operations/unary_operation_executor.h"
#include "execution/sql/vector_operations/vector_operations.h"
#include "execution/sql/vector_projection_iterator.h"
#include "execution/util/bit_util.h"
#include "execution/util/cpu_info.h"
#include "execution/util/timer.h"
#include "loggers/execution_logger.h"
#include "spdlog/fmt/fmt.h"

namespace terrier::execution::sql {

class AggregationHashTable::HashToGroupIdMap {
  // Marker indicating an empty slot in the hash table
  static constexpr const uint16_t EMPTY = std::numeric_limits<uint16_t>::max();

 public:
  // An entry in the hash table.
  struct Entry {
    uint16_t gid_;
    uint16_t next_;
  };

  HashToGroupIdMap() {
    const uint64_t max_size = common::Constants::K_DEFAULT_VECTOR_SIZE;
    capacity_ = max_size * 2;
    mask_ = capacity_ - 1;
    entries_ = std::unique_ptr<uint16_t[]>(new uint16_t[capacity_]{EMPTY});
    storage_ = std::make_unique<Entry[]>(max_size);
    storage_used_ = 0;
  }

  // This class cannot be copied or moved.
  DISALLOW_COPY_AND_MOVE(HashToGroupIdMap);

  // Remove all elements from the hash table.
  void Clear() {
    storage_used_ = 0;
    std::memset(entries_.get(), EMPTY, capacity_ * sizeof(uint16_t));
  }

  // Find the group associated to the input hash, but only if the predicate is
  // true. If no such value is found, return a nullptr.
  template <typename P>
  uint16_t *Find(const hash_t hash, P p) {
    uint16_t candidate = entries_[hash & mask_];
    if (candidate == EMPTY) {
      return nullptr;
    }
    for (auto candidate_ptr = &storage_[candidate]; candidate != EMPTY;
         candidate = candidate_ptr->next_, candidate_ptr = &storage_[candidate]) {
      if (p(candidate_ptr->gid_)) {
        return &candidate_ptr->gid_;
      }
    }
    return nullptr;
  }

  // Insert a new hash-group mapping.
  void Insert(const hash_t hash, const uint16_t gid) {
    TERRIER_ASSERT(storage_used_ < common::Constants::K_DEFAULT_VECTOR_SIZE, "Too many elements in table");

    // Determine the spot in the storage the new entry occupies.
    uint16_t entry_pos = storage_used_++;
    Entry *entry = &storage_[entry_pos];

    // Put the new entry at the head of the chain.
    entry->next_ = entries_[hash & mask_];
    entries_[hash & mask_] = entry_pos;

    // Fill the group ID.
    entry->gid_ = gid;
  }

  // Iterators.
  Entry *begin() { return storage_.get(); }                // NOLINT to match C++ iterators
  Entry *end() { return storage_.get() + storage_used_; }  // NOLINT to match C++ iterators

 private:
  // The mask to use to map hashes to slots in the 'entries' array.
  hash_t mask_;
  // The main entries directory mapping to indexes of storage slots.
  std::unique_ptr<uint16_t[]> entries_;
  // Main array storage of hash table data (keys, values, hashes, etc.)
  std::unique_ptr<Entry[]> storage_;
  // The capacity of the directory.
  uint16_t capacity_;
  // The number of slots of storage that have been used.
  uint16_t storage_used_;
};

// ---------------------------------------------------------
// Batch Process State
// ---------------------------------------------------------

AggregationHashTable::BatchProcessState::BatchProcessState(std::unique_ptr<libcount::HLL> estimator,
                                                           std::unique_ptr<HashToGroupIdMap> hash_to_group_map)
    : hll_estimator_(std::move(estimator)),
      hash_to_group_map_(std::move(hash_to_group_map)),
      groups_not_found_(common::Constants::K_DEFAULT_VECTOR_SIZE),
      groups_found_(common::Constants::K_DEFAULT_VECTOR_SIZE),
      key_not_equal_(common::Constants::K_DEFAULT_VECTOR_SIZE),
      key_equal_(common::Constants::K_DEFAULT_VECTOR_SIZE) {
  hash_and_entries_.Initialize({TypeId::Hash, TypeId::Pointer});
}

AggregationHashTable::BatchProcessState::~BatchProcessState() = default;

void AggregationHashTable::BatchProcessState::Reset(VectorProjectionIterator *input_batch) {
  // Resize the lists if they don't match the input. This should only happen
  // once, on the last input batch where the size may be less than one full
  // vector.
  if (const auto count = input_batch->GetTotalTupleCount(); count != hash_and_entries_.GetTotalTupleCount()) {
    hash_and_entries_.Reset(count);
    groups_not_found_.Resize(count);
    groups_found_.Resize(count);
    key_not_equal_.Resize(count);
    key_equal_.Resize(count);
  }

  // Initially, there are no groups found and all keys are unequal.
  input_batch->GetVectorProjection()->CopySelectionsTo(&groups_not_found_);
  input_batch->GetVectorProjection()->CopySelectionsTo(&key_not_equal_);

  // Clear the rest of the lists.
  groups_found_.Clear();
  key_equal_.Clear();

  // Clear the collision table.
  hash_to_group_map_->Clear();
}

// ---------------------------------------------------------
// Aggregation Hash Table
// ---------------------------------------------------------

AggregationHashTable::AggregationHashTable(const exec::ExecutionSettings &exec_settings,
                                           exec::ExecutionContext *exec_ctx, const std::size_t payload_size,
                                           const uint32_t initial_size)
    : exec_settings_(exec_settings),
      exec_ctx_(exec_ctx),
      memory_(exec_ctx->GetMemoryPool()),
      payload_size_(payload_size),
      entries_(HashTableEntry::ComputeEntrySize(payload_size_), MemoryPoolAllocator<byte>(memory_)),
      owned_entries_(memory_),
      hash_table_(DEFAULT_LOAD_FACTOR),
      batch_state_(nullptr),
      merge_partition_fn_(nullptr),
      partition_heads_(nullptr),
      partition_tails_(nullptr),
      partition_estimates_(nullptr),
      partition_tables_(nullptr),
      partition_shift_bits_(util::BitUtil::CountLeadingZeros(uint64_t(DEFAULT_NUM_PARTITIONS) - 1)) {
  hash_table_.SetSize(initial_size, memory_->GetTracker());
  max_fill_ = std::llround(hash_table_.GetCapacity() * hash_table_.GetLoadFactor());

  // Compute flush threshold. In partitioned mode, we want the thread-local
  // pre-aggregation hash table to be sized to fit in cache. Target L2.
  const uint64_t l2_size = CpuInfo::Instance()->GetCacheSize(CpuInfo::L2_CACHE);
  flush_threshold_ = std::llround(static_cast<float>(l2_size) / entries_.ElementSize() * DEFAULT_LOAD_FACTOR);
  flush_threshold_ = std::max(uint64_t{256}, common::MathUtil::PowerOf2Floor(flush_threshold_));
}

AggregationHashTable::AggregationHashTable(const exec::ExecutionSettings &exec_settings,
                                           exec::ExecutionContext *exec_ctx, std::size_t payload_size)
    : AggregationHashTable(exec_settings, exec_ctx, payload_size, DEFAULT_INITIAL_TABLE_SIZE) {}

AggregationHashTable::~AggregationHashTable() {
  if (batch_state_ != nullptr) {
    memory_->DeleteObject(std::move(batch_state_));
  }
  if (partition_heads_ != nullptr) {
    memory_->DeallocateArray(partition_heads_, DEFAULT_NUM_PARTITIONS);
  }
  if (partition_tails_ != nullptr) {
    memory_->DeallocateArray(partition_tails_, DEFAULT_NUM_PARTITIONS);
  }
  if (partition_estimates_ != nullptr) {
    // The estimates array uses HLL instances acquired from libcount. We own
    // them so we have to delete them manually.
    for (uint32_t i = 0; i < DEFAULT_NUM_PARTITIONS; i++) {
      delete partition_estimates_[i];
    }
    memory_->DeallocateArray(partition_estimates_, DEFAULT_NUM_PARTITIONS);
  }
  if (partition_tables_ != nullptr) {
    for (uint32_t i = 0; i < DEFAULT_NUM_PARTITIONS; i++) {
      if (partition_tables_[i] != nullptr) {
        partition_tables_[i]->~AggregationHashTable();
        memory_->Deallocate(partition_tables_[i], sizeof(AggregationHashTable));
      }
    }
    memory_->DeallocateArray(partition_tables_, DEFAULT_NUM_PARTITIONS);
  }
}

void AggregationHashTable::Grow() {
  // Resize table
  const uint64_t new_size = hash_table_.GetCapacity() * 2;
  hash_table_.SetSize(new_size, memory_->GetTracker());
  max_fill_ = std::llround(hash_table_.GetCapacity() * hash_table_.GetLoadFactor());

  // Insert elements again
  for (byte *untyped_entry : entries_) {
    auto *entry = reinterpret_cast<HashTableEntry *>(untyped_entry);
    hash_table_.Insert<false>(entry);
  }

  // Update stats
  stats_.num_growths_++;
}

HashTableEntry *AggregationHashTable::AllocateEntryInternal(const hash_t hash) {
  // Allocate an entry
  auto *entry = reinterpret_cast<HashTableEntry *>(entries_.Append());
  entry->hash_ = hash;
  entry->next_ = nullptr;

  // Insert into table
  hash_table_.Insert<false>(entry);

  // Done
  return entry;
}

byte *AggregationHashTable::AllocInputTuple(const hash_t hash) {
  stats_.num_inserts_++;

  // Grow if need be
  if (NeedsToGrow()) {
    Grow();
  }

  // Allocate an entry
  HashTableEntry *entry = AllocateEntryInternal(hash);

  // Return the payload so the client can write into it
  return entry->payload_;
}

void AggregationHashTable::AllocateOverflowPartitions() {
  TERRIER_ASSERT((partition_heads_ == nullptr) == (partition_tails_ == nullptr),
                 "Head and tail of overflow partitions list are not equally allocated");

  if (partition_heads_ == nullptr) {
    partition_heads_ = memory_->AllocateArray<HashTableEntry *>(DEFAULT_NUM_PARTITIONS, true);
    partition_tails_ = memory_->AllocateArray<HashTableEntry *>(DEFAULT_NUM_PARTITIONS, true);
    partition_estimates_ = memory_->AllocateArray<libcount::HLL *>(DEFAULT_NUM_PARTITIONS, false);
    for (uint32_t i = 0; i < DEFAULT_NUM_PARTITIONS; i++) {
      partition_estimates_[i] = libcount::HLL::Create(DEFAULT_HLL_PRECISION).release();
    }
    partition_tables_ = memory_->AllocateArray<AggregationHashTable *>(DEFAULT_NUM_PARTITIONS, true);
  }
}

void AggregationHashTable::FlushToOverflowPartitions() {
  if (UNLIKELY(partition_heads_ == nullptr)) {
    AllocateOverflowPartitions();
  }

  // Dump all entries from the hash table into the overflow partitions. For each
  // entry in the table, we compute its destination partition using the highest
  // log(P) bits for P partitions. For each partition, we also track an estimate
  // of the number of unique hash values so that when the partitions are merged,
  // we can appropriately size the table before merging. The issue is that both
  // the bits used for partition selection and unique hash estimation are the
  // same! Thus, the estimates are inaccurate. To solve this, we scramble the
  // hash values using a bijective hash scrambling before feeding them to the
  // estimator.

  hash_table_.FlushEntries([this](HashTableEntry *entry) {
    const uint64_t partition_idx = (entry->hash_ >> partition_shift_bits_);
    entry->next_ = partition_heads_[partition_idx];
    partition_heads_[partition_idx] = entry;
    if (UNLIKELY(partition_tails_[partition_idx] == nullptr)) {
      partition_tails_[partition_idx] = entry;
    }
    partition_estimates_[partition_idx]->Update(common::HashUtil::ScrambleHash(entry->hash_));
  });

  // Update stats
  stats_.num_flushes_++;
}

byte *AggregationHashTable::AllocInputTuplePartitioned(hash_t hash) {
  byte *ret = AllocInputTuple(hash);
  if (NeedsToFlushToOverflowPartitions()) {
    FlushToOverflowPartitions();
  }
  return ret;
}

void AggregationHashTable::ComputeHash(VectorProjectionIterator *input_batch,
                                       const std::vector<uint32_t> &key_indexes) {
  input_batch->GetVectorProjection()->Hash(key_indexes, batch_state_->Hashes());
}

void AggregationHashTable::LookupInitial() {
  // Probe the hash table for every active hash in the hashes vector. Store the
  // result in the entries vector, copying NULLs and the filter, too.
  // TODO(pmenon): Move bulk lookup into ChainingHashTable? Batch lookups should
  //               use SIMD gathers if hashes vector is full. For some reason it
  //               isn't. Investigate why.
  UnaryOperationExecutor::Execute<hash_t, const HashTableEntry *>(
      exec_settings_, *batch_state_->Hashes(),
      batch_state_->Entries(), [&](const hash_t hash) noexcept { return hash_table_.FindChainHead(hash); });

  // Find non-null entries whose keys must be checked and place them in the
  // key-not-equal list which is used during key equality checking.
  ConstantVector null_ptr(GenericValue::CreatePointer(0));
  VectorOps::SelectNotEqual(exec_settings_, *batch_state_->Entries(), null_ptr, batch_state_->KeyNotEqual());
}

void AggregationHashTable::CheckKeyEquality(VectorProjectionIterator *input_batch,
                                            const std::vector<uint32_t> &key_indexes) {
  // The list of tuples whose keys need to be checked is stored in
  // key-not-equal. We copy it to the key-equal list which we'll use as the
  // running list of tuples that DO have matching keys to table aggregates.
  batch_state_->KeyEqual()->AssignFrom(*batch_state_->KeyNotEqual());

  // If no tuples to check, we can exit.
  if (batch_state_->KeyEqual()->IsEmpty()) {
    return;
  }

  // Check all key components one at a time.
  std::size_t key_offset = HashTableEntry::ComputePayloadOffset();
  for (const auto key_index : key_indexes) {
    const Vector *key_vector = input_batch->GetVectorProjection()->GetColumn(key_index);
    VectorOps::GatherAndSelectEqual(*key_vector, *batch_state_->Entries(), key_offset, batch_state_->KeyEqual());
    key_offset += GetTypeIdSize(key_vector->GetTypeId());
  }

  // The key-equal list now contains the TIDs of all tuples that succeeded in
  // matching keys with their associated group. Add them to the running list
  // of groups-found.
  batch_state_->GroupsFound()->UnionWith(*batch_state_->KeyEqual());

  // Tuples that didn't match keys need to proceed through the chain of
  // candidate entries. Collect them in the key-not-equal list.
  batch_state_->KeyNotEqual()->UnsetFrom(*batch_state_->KeyEqual());
}

void AggregationHashTable::FollowNext() {
  auto *raw_entries = reinterpret_cast<HashTableEntry **>(batch_state_->Entries()->GetData());
  batch_state_->KeyNotEqual()->Filter([&](uint64_t i) { return (raw_entries[i] = raw_entries[i]->next_) != nullptr; });
}

void AggregationHashTable::FindGroups(VectorProjectionIterator *input_batch, const std::vector<uint32_t> &key_indexes) {
  // Perform initial lookup.
  LookupInitial();

  // Check keys.
  CheckKeyEquality(input_batch, key_indexes);

  // While we have unmatched keys, move along chain.
  while (!batch_state_->KeyNotEqual()->IsEmpty()) {
    FollowNext();
    CheckKeyEquality(input_batch, key_indexes);
  }
}

namespace {

template <typename T, typename F>
void TemplatedFixGrouping(AggregationHashTable::HashToGroupIdMap *hash_to_group_map, const Vector &hashes,
                          const Vector &entries, const Vector &probe_keys, TupleIdList *tid_list, F f) {
  auto *RESTRICT raw_hashes = reinterpret_cast<const hash_t *>(hashes.GetData());
  auto *RESTRICT raw_entries = reinterpret_cast<const HashTableEntry **>(entries.GetData());
  auto *RESTRICT raw_keys = reinterpret_cast<const T *>(probe_keys.GetData());
  tid_list->Filter([&](const uint64_t i) {
    auto *gid =
        hash_to_group_map->Find(raw_hashes[i], [&](const uint16_t gid) { return raw_keys[gid] == raw_keys[i]; });
    if (gid != nullptr) {
      raw_entries[i] = raw_entries[*gid];
    } else {
      hash_to_group_map->Insert(raw_hashes[i], i);
      raw_entries[i] = f(raw_hashes[i]);
    }
    return gid != nullptr;
  });
}

template <typename F>
void FixGrouping(AggregationHashTable::HashToGroupIdMap *groups, const Vector &hashes, const Vector &entries,
                 const Vector &probe_keys, TupleIdList *tid_list, F f) {
  switch (probe_keys.GetTypeId()) {
    case TypeId::Boolean:
      TemplatedFixGrouping<bool>(groups, hashes, entries, probe_keys, tid_list, f);
      break;
    case TypeId::TinyInt:
      TemplatedFixGrouping<int8_t>(groups, hashes, entries, probe_keys, tid_list, f);
      break;
    case TypeId::SmallInt:
      TemplatedFixGrouping<int16_t>(groups, hashes, entries, probe_keys, tid_list, f);
      break;
    case TypeId::Integer:
      TemplatedFixGrouping<int32_t>(groups, hashes, entries, probe_keys, tid_list, f);
      break;
    case TypeId::BigInt:
      TemplatedFixGrouping<int64_t>(groups, hashes, entries, probe_keys, tid_list, f);
      break;
    case TypeId::Float:
      TemplatedFixGrouping<float>(groups, hashes, entries, probe_keys, tid_list, f);
      break;
    case TypeId::Double:
      TemplatedFixGrouping<double>(groups, hashes, entries, probe_keys, tid_list, f);
      break;
    case TypeId::Date:
      TemplatedFixGrouping<Date>(groups, hashes, entries, probe_keys, tid_list, f);
      break;
    case TypeId::Timestamp:
      TemplatedFixGrouping<Timestamp>(groups, hashes, entries, probe_keys, tid_list, f);
      break;
    case TypeId::Varchar:
      TemplatedFixGrouping<storage::VarlenEntry>(groups, hashes, entries, probe_keys, tid_list, f);
      break;
    case TypeId::Varbinary:
      TemplatedFixGrouping<Blob>(groups, hashes, entries, probe_keys, tid_list, f);
      break;
    default:
      throw NOT_IMPLEMENTED_EXCEPTION("Unsupported type for key comparison.");
  }
}

}  // namespace

void AggregationHashTable::CreateMissingGroups(VectorProjectionIterator *input_batch,
                                               const std::vector<uint32_t> &key_indexes,
                                               const AggregationHashTable::VectorInitAggFn init_agg_fn) {
  // The groups-found list contains all tuples that found a matching group in
  // the aggregation hash table. Thus, the list of tuples that did not find a
  // match is the complement of the groups-found list.
  batch_state_->GroupsNotFound()->UnsetFrom(*batch_state_->GroupsFound());

  // If all tuples found a matching group, we don't need to create any.
  if (batch_state_->GroupsNotFound()->IsEmpty()) {
    return;
  }

  // Find and resolve duplicate keys in this batch.
  batch_state_->KeyNotEqual()->AssignFrom(*batch_state_->GroupsNotFound());
  batch_state_->KeyEqual()->AssignFrom(*batch_state_->GroupsNotFound());
  for (const auto key_index : key_indexes) {
    const Vector *key_vector = input_batch->GetVectorProjection()->GetColumn(key_index);
    FixGrouping(batch_state_->HashToGroupMap(),  // Hash-to-group mapping
                *batch_state_->Hashes(),         // Hashes
                *batch_state_->Entries(),        // Entries
                *key_vector,                     // Keys
                batch_state_->KeyEqual(),        // The running list of tuples that found a match
                [this](const hash_t hash) { return AllocateEntryInternal(hash); });
  }

  // The key-not-equal list contains the list of all TIDs that did not find a
  // matching group. The key-equal list contains TIDs tuples that found a
  // matching group WITHIN this batch. The difference between these lists is
  // the list of tuples that require NEW groups.
  batch_state_->KeyNotEqual()->UnsetFrom(*batch_state_->KeyEqual());

  // Let the initialization function handle all the newly created aggregates.
  VectorProjectionIterator iter(batch_state_->Projection(), batch_state_->KeyNotEqual());
  input_batch->SetVectorProjection(input_batch->GetVectorProjection(), batch_state_->KeyNotEqual());
  init_agg_fn(&iter, input_batch);

  // All new aggregates have been created. Add in the new groups into found
  // groups; this is the final list.
  batch_state_->GroupsFound()->UnionWith(*batch_state_->GroupsNotFound());
}

void AggregationHashTable::AdvanceGroups(VectorProjectionIterator *input_batch,
                                         const AggregationHashTable::VectorAdvanceAggFn advance_agg_fn) {
  // Let the callback handle updating the aggregates.
  VectorProjectionIterator iter(batch_state_->Projection(), batch_state_->GroupsFound());
  input_batch->SetVectorProjection(input_batch->GetVectorProjection(), batch_state_->GroupsFound());
  advance_agg_fn(&iter, input_batch);
}

void AggregationHashTable::ProcessBatch(VectorProjectionIterator *input_batch, const std::vector<uint32_t> &key_indexes,
                                        const AggregationHashTable::VectorInitAggFn init_agg_fn,
                                        const AggregationHashTable::VectorAdvanceAggFn advance_agg_fn,
                                        const bool partitioned_aggregation) {
  // No-op if the iterator is empty.
  if (input_batch->IsEmpty()) {
    return;
  }

  // Initialize the batch state if need be. Note: this is only performed once.
  if (UNLIKELY(batch_state_ == nullptr)) {
    batch_state_ = memory_->MakeObject<BatchProcessState>(
        libcount::HLL::Create(DEFAULT_HLL_PRECISION),  // The Hyper-Log-Log estimator
        std::make_unique<HashToGroupIdMap>());         // The Hash-to-GroupID map
  }

  // Reset state for the incoming batch.
  batch_state_->Reset(input_batch);

  // Compute the hashes.
  ComputeHash(input_batch, key_indexes);

  // Find groups.
  FindGroups(input_batch, key_indexes);

  // Creating missing groups.
  CreateMissingGroups(input_batch, key_indexes, init_agg_fn);

  // If the caller requested a partitioned aggregation, drain the main hash
  // table out to the overflow partitions, but only if needed.
  if (partitioned_aggregation) {
    if (NeedsToFlushToOverflowPartitions()) {
      FlushToOverflowPartitions();
    }
  } else {
    if (NeedsToGrow()) {
      Grow();
    }
  }

  // Advance the aggregates for all tuples that found a match.
  AdvanceGroups(input_batch, advance_agg_fn);
}

void AggregationHashTable::TransferMemoryAndPartitions(ThreadStateContainer *thread_states, std::size_t agg_ht_offset,
                                                       MergePartitionFn merge_partition_fn) {
  auto pre_hook = static_cast<uint32_t>(HookOffsets::StartHook);
  auto post_hook = static_cast<uint32_t>(HookOffsets::EndHook);
  auto *tls = thread_states->AccessCurrentThreadState();
  exec_ctx_->InvokeHook(pre_hook, tls, nullptr);

  // Set the partition merging function. This function tells us how to merge a
  // set of overflow partitions into an AggregationHashTable.
  merge_partition_fn_ = merge_partition_fn;

  // Allocate the set of overflow partitions so that we can link in all
  // thread-local overflow partitions to us.
  AllocateOverflowPartitions();

  // If, by chance, we have some un-flushed aggregate data, flush it out now to
  // ensure partitioned build captures it.
  if (GetTupleCount() > 0) {
    FlushToOverflowPartitions();
  }

  // Okay, now we actually pull out the thread-local aggregation hash tables and
  // move both their main entry data and the overflow partitions to us.
  std::vector<AggregationHashTable *> tl_agg_ht;
  thread_states->CollectThreadLocalStateElementsAs(&tl_agg_ht, agg_ht_offset);
  for (auto *table : tl_agg_ht) {
    // Flush each table to ensure their hash tables are empty and their overflow
    // partitions contain all partial aggregates
    stats_.num_inserts_ += table->stats_.num_inserts_;
    table->FlushToOverflowPartitions();

    // Now, move over their memory
    owned_entries_.emplace_back(std::move(table->entries_));

    TERRIER_ASSERT(table->owned_entries_.empty(),
                   "A thread-local aggregation table should not have any owned "
                   "entries themselves. Nested/recursive aggregations not supported.");

    // Now, move over their overflow partitions list
    for (uint32_t part_idx = 0; part_idx < DEFAULT_NUM_PARTITIONS; part_idx++) {
      if (table->partition_heads_[part_idx] != nullptr) {
        // Link in the partition list
        table->partition_tails_[part_idx]->next_ = partition_heads_[part_idx];
        partition_heads_[part_idx] = table->partition_heads_[part_idx];
        if (partition_tails_[part_idx] == nullptr) {
          partition_tails_[part_idx] = table->partition_tails_[part_idx];
        }
        // Update the partition's unique-count estimate
        partition_estimates_[part_idx]->Merge(table->partition_estimates_[part_idx]);
      }
    }
  }

  exec_ctx_->InvokeHook(post_hook, tls, reinterpret_cast<void *>(tl_agg_ht.size()));
}

AggregationHashTable *AggregationHashTable::GetOrBuildTableOverPartition(void *query_state,
                                                                         const uint32_t partition_idx) {
  TERRIER_ASSERT(partition_idx < DEFAULT_NUM_PARTITIONS, "Out-of-bounds partition access");
  TERRIER_ASSERT(partition_heads_[partition_idx] != nullptr,
                 "Should not build aggregation table over empty partition!");
  TERRIER_ASSERT(merge_partition_fn_ != nullptr,
                 "Merging function was not provided! Did you forget to call TransferMemoryAndPartitions()?");

  // If the table has already been built, return it
  if (partition_tables_[partition_idx] != nullptr) {
    return partition_tables_[partition_idx];
  }

  // Create it
  auto estimated_size = partition_estimates_[partition_idx]->Estimate();
  auto *agg_table = new (memory_->AllocateAligned(sizeof(AggregationHashTable), alignof(AggregationHashTable), false))
      AggregationHashTable(exec_settings_, exec_ctx_, payload_size_, estimated_size);

  util::Timer<std::milli> timer;
  timer.Start();

  // Build it
  AHTOverflowPartitionIterator iter(partition_heads_ + partition_idx, partition_heads_ + partition_idx + 1);
  merge_partition_fn_(query_state, agg_table, &iter);

  timer.Stop();
  EXECUTION_LOG_DEBUG("Overflow Partition {}: estimated size = {}, actual size = {}, build time = {:2f} ms",
                      partition_idx, estimated_size, agg_table->GetTupleCount(), timer.GetElapsed());

  // Set it
  partition_tables_[partition_idx] = agg_table;

  // Return it
  return agg_table;
}

void AggregationHashTable::ExecutePartitionedScan(void *query_state, AggregationHashTable::ScanPartitionFn scan_fn) {
  TERRIER_ASSERT(partition_heads_ != nullptr && merge_partition_fn_ != nullptr,
                 "No overflow partitions allocated, or no merging function allocated. Did you call "
                 "TransferMemoryAndPartitions() before issuing the partitioned scan?");

  // Determine the non-empty overflow partitions.
  for (uint32_t part_idx = 0; part_idx < DEFAULT_NUM_PARTITIONS; part_idx++) {
    if (partition_heads_[part_idx] != nullptr) {
      // Get or build the table on the partition.
      auto agg_table_partition = GetOrBuildTableOverPartition(query_state, part_idx);
      // Scan the partition.
      scan_fn(query_state, nullptr, agg_table_partition);
    }
  }
}

void AggregationHashTable::ExecuteParallelPartitionedScan(void *query_state, ThreadStateContainer *thread_states,
                                                          const AggregationHashTable::ScanPartitionFn scan_fn) {
  // At this point, this aggregation table has a list of overflow partitions
  // that must be merged into a single aggregation hash table. For simplicity,
  // we create a new aggregation hash table per overflow partition, and merge
  // the contents of that partition into the new hash table. We use the HLL
  // estimates to size the hash table before construction so as to minimize the
  // growth factor. Each aggregation hash table partition will be built and
  // scanned in parallel.

  TERRIER_ASSERT(partition_heads_ != nullptr && merge_partition_fn_ != nullptr,
                 "No overflow partitions allocated, or no merging function allocated. Did you call "
                 "TransferMemoryAndPartitions() before issuing the partitioned scan?");

  // Determine the non-empty overflow partitions
  std::vector<uint32_t> nonempty_parts;
  nonempty_parts.reserve(DEFAULT_NUM_PARTITIONS);
  for (uint32_t i = 0; i < DEFAULT_NUM_PARTITIONS; i++) {
    if (partition_heads_[i] != nullptr) {
      nonempty_parts.push_back(i);
    }
  }

  util::Timer<std::milli> timer;
  timer.Start();

  size_t num_threads = tbb::task_scheduler_init::default_num_threads();
  size_t num_tasks = nonempty_parts.size();
  size_t concurrent_estimate = std::min(num_threads, num_tasks);
  exec_ctx_->SetNumConcurrentEstimate(concurrent_estimate);

  tbb::parallel_for_each(nonempty_parts, [&](const uint32_t part_idx) {
    // TODO(wz2): Resource trackers are started and stopped within scan_fn. It might be more correct
    // to start the trackers here manually -- or have TransferMemoryAndPartitions build all the tables
    // over each partition (but that would require storing the agg table pointers).

    // Build a hash table over the given partition
    auto agg_table_partition = GetOrBuildTableOverPartition(query_state, part_idx);

    // Get a handle to the thread-local state of the executing thread
    auto thread_state = thread_states->AccessCurrentThreadState();

    // Scan the partition
    scan_fn(query_state, thread_state, agg_table_partition);
  });

  exec_ctx_->SetNumConcurrentEstimate(0);
  timer.Stop();

  const uint64_t tuple_count =
      std::accumulate(nonempty_parts.begin(), nonempty_parts.end(), uint64_t{0},
                      [&](const auto curr, const auto idx) { return curr + partition_tables_[idx]->GetTupleCount(); });

  UNUSED_ATTRIBUTE double tps = (tuple_count / timer.GetElapsed()) / 1000.0;
  EXECUTION_LOG_TRACE("Built and scanned {} tables totalling {} tuples in {:.2f} ms ({:.2f} mtps)",
                      nonempty_parts.size(), tuple_count, timer.GetElapsed(), tps);
}

void AggregationHashTable::BuildAllPartitions(void *query_state) {
  TERRIER_ASSERT(partition_tables_ == nullptr, "Should not have built aggregation hash tables already");
  partition_tables_ = memory_->AllocateArray<AggregationHashTable *>(DEFAULT_NUM_PARTITIONS, true);

  // Find non-empty partitions.
  std::vector<uint32_t> nonempty_parts;
  nonempty_parts.reserve(DEFAULT_NUM_PARTITIONS);
  for (uint32_t part_idx = 0; part_idx < DEFAULT_NUM_PARTITIONS; part_idx++) {
    if (partition_heads_[part_idx] != nullptr) {
      nonempty_parts.push_back(part_idx);
    }
  }

  // For each valid partition, build a hash table over its contents.
  tbb::parallel_for_each(nonempty_parts,
                         [&](const uint32_t part_idx) { GetOrBuildTableOverPartition(query_state, part_idx); });
}

void AggregationHashTable::Repartition() {
  // Find all non-empty partitions.
  std::vector<AggregationHashTable *> nonempty_tables;
  nonempty_tables.reserve(DEFAULT_NUM_PARTITIONS);
  for (uint32_t part_idx = 0; part_idx < DEFAULT_NUM_PARTITIONS; part_idx++) {
    if (partition_tables_[part_idx] != nullptr) {
      nonempty_tables.push_back(partition_tables_[part_idx]);
    }
  }

  // First, flush all hash table partitions to their own overflow buckets.
  tbb::parallel_for_each(nonempty_tables, [&](auto table) { table->FlushToOverflowPartitions(); });

  // Now, transfer each hash table partition's overflow buckets to us.
  for (auto *table : nonempty_tables) {
    for (uint32_t part_idx = 0; part_idx < DEFAULT_NUM_PARTITIONS; part_idx++) {
      if (table->partition_heads_[part_idx] != nullptr) {
        table->partition_tails_[part_idx]->next_ = partition_heads_[part_idx];
        partition_heads_[part_idx] = table->partition_heads_[part_idx];
        if (partition_tails_[part_idx] == nullptr) {
          partition_tails_[part_idx] = table->partition_tails_[part_idx];
        }
      }
    }

    // Move partitioned hash table memory into this main hash table.
    owned_entries_.emplace_back(std::move(table->entries_));
  }
}

void AggregationHashTable::MergePartitions(AggregationHashTable *target, void *query_state,
                                           AggregationHashTable::MergePartitionFn merge_func) {
  if (target->partition_tables_ == nullptr) {
    target->partition_tables_ = memory_->AllocateArray<AggregationHashTable *>(DEFAULT_NUM_PARTITIONS, true);
  }

  // Find non-empty partitions.
  std::vector<uint32_t> nonempty_parts;
  nonempty_parts.reserve(DEFAULT_NUM_PARTITIONS);
  for (uint32_t part_idx = 0; part_idx < DEFAULT_NUM_PARTITIONS; part_idx++) {
    if (partition_heads_[part_idx] != nullptr) {
      nonempty_parts.push_back(part_idx);
    }
  }

  // Merge overflow data into the appropriate partitioned table in the target.
  tbb::parallel_for_each(nonempty_parts, [&](const uint32_t part_idx) {
    // Get the partitioned hash table from the target.
    auto agg_table_partition = target->GetOrBuildTableOverPartition(query_state, part_idx);

    // Merge our overflow partition into target table.
    AHTOverflowPartitionIterator iter(partition_heads_ + part_idx, partition_heads_ + part_idx + 1);
    merge_func(query_state, agg_table_partition, &iter);
  });

  // Move our memory to the target.
  target->owned_entries_.emplace_back(std::move(entries_));
}

}  // namespace terrier::execution::sql
