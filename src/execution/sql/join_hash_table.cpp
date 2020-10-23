#include "execution/sql/join_hash_table.h"

#include <llvm/ADT/STLExtras.h>
#include <tbb/parallel_for_each.h>
#include <tbb/task_scheduler_init.h>

#include <algorithm>
#include <limits>
#include <utility>
#include <vector>

#include "count/hll.h"
#include "execution/exec/execution_context.h"
#include "execution/sql/memory_pool.h"
#include "execution/sql/thread_state_container.h"
#include "execution/sql/vector.h"
#include "execution/sql/vector_operations/unary_operation_executor.h"
#include "execution/util/cpu_info.h"
#include "execution/util/memory.h"
#include "execution/util/timer.h"
#include "loggers/execution_logger.h"

namespace terrier::execution::sql {

JoinHashTable::JoinHashTable(const exec::ExecutionSettings &exec_settings, exec::ExecutionContext *exec_ctx,
                             uint32_t tuple_size, bool use_concise_ht)
    : exec_settings_(exec_settings),
      exec_ctx_(exec_ctx),
      entries_(HashTableEntry::ComputeEntrySize(tuple_size), MemoryPoolAllocator<byte>(exec_ctx->GetMemoryPool())),
      owned_(exec_ctx->GetMemoryPool()),
      concise_hash_table_(0),
      hll_estimator_(libcount::HLL::Create(DEFAULT_HLL_PRECISION)),
      built_(false),
      use_concise_ht_(use_concise_ht),
      tracker_(exec_ctx->GetMemoryPool()->GetTracker()) {}

// Needed because we forward-declared HLL from libcount
JoinHashTable::~JoinHashTable() = default;

byte *JoinHashTable::AllocInputTuple(const hash_t hash) {
  // Add to unique_count estimation
  hll_estimator_->Update(hash);

  // Allocate space for a new tuple
  auto *entry = reinterpret_cast<HashTableEntry *>(entries_.Append());
  entry->hash_ = hash;
  entry->next_ = nullptr;
  return entry->payload_;
}

void JoinHashTable::BuildChainingHashTable() {
  // Perfectly size the generic hash table in preparation for bulk-load.
  chaining_hash_table_.SetSize(GetTupleCount(), tracker_);

  // Bulk-load the, now correctly sized, generic hash table using a non-concurrent algorithm.
  chaining_hash_table_.InsertBatch<false>(&entries_);

#ifndef NDEBUG
  const auto [min, max, avg] = chaining_hash_table_.GetChainLengthStats();
  (void)min;
  (void)max;
  (void)avg;
  EXECUTION_LOG_DEBUG("ChainingHashTable chain stats: min={}, max={}, avg={}", min, max, avg);
#endif
}

namespace {

// The bits we set in the entry to mark if the entry has been buffered in the
// reorder buffer and whether the entry has been processed (i.e., if the entry
// is in its final location in either the main or overflow arenas).
constexpr const uint64_t BUFFERED_BIT = 1ull << 62ull;
constexpr const uint64_t PROCESSED_BIT = 1ull << 63ull;

// A reorder is a small piece of buffer space into which we temporarily buffer
// hash table entries for the purposes of reordering them.
class ReorderBuffer {
 public:
  // Use a 16 KB internal buffer for temporary copies
  static constexpr const uint32_t BUFFER_SIZE_IN_BYTES = 16 * 1024;

  ReorderBuffer(util::ChunkedVector<MemoryPoolAllocator<byte>> *entries, uint64_t max_elems, uint64_t begin_read_idx,
                uint64_t end_read_idx)
      : entry_size_(entries->ElementSize()),
        buf_idx_(0),
        max_elems_(std::min(max_elems, BUFFER_SIZE_IN_BYTES / entry_size_) - 1),
        temp_buf_(buffer_ + (max_elems_ * entry_size_)),
        read_idx_(begin_read_idx),
        end_read_idx_(end_read_idx),
        entries_(entries) {}

  // This class cannot be copied or moved
  DISALLOW_COPY_AND_MOVE(ReorderBuffer);

  // Return a pointer to the element at the given index in the buffer.
  template <typename T = byte>
  T *BufEntryAt(uint64_t idx) {
    return reinterpret_cast<T *>(buffer_ + (idx * entry_size_));
  }

  // Has the given entry been processed? In other words, is the input entry in
  // its final location in the entry array?
  bool IsProcessed(const HashTableEntry *entry) const { return (entry->cht_slot_ & PROCESSED_BIT) != 0u; }

  // Mark the given entry as processed and in its final location.
  void SetProcessed(HashTableEntry *entry) const { entry->cht_slot_ |= PROCESSED_BIT; }

  // Has the given entry been buffered in this reorder buffer?
  bool IsBuffered(const HashTableEntry *entry) const { return (entry->cht_slot_ & BUFFERED_BIT) != 0u; }

  // Mark the given entry as buffered in the reorder buffer.
  void SetBuffered(HashTableEntry *entry) const { entry->cht_slot_ |= BUFFERED_BIT; }

  // Fill this reorder buffer with as many entries as possible. Each entry that
  // is inserted is marked/tagged as buffered.
  bool Fill() {
    while (buf_idx_ < max_elems_ && read_idx_ < end_read_idx_) {
      auto *entry = reinterpret_cast<HashTableEntry *>((*entries_)[read_idx_++]);

      if (IsProcessed(entry)) {
        continue;
      }

      byte *const buf_space = BufEntryAt(buf_idx_++);
      std::memcpy(buf_space, static_cast<void *>(entry), entry_size_);
      SetBuffered(entry);
    }

    return buf_idx_ > 0;
  }

  // Reset the index where the next buffered entry goes.
  void Reset(const uint64_t new_buf_idx) { buf_idx_ = new_buf_idx; }

  // Return the number of currently buffered entries
  uint64_t GetNumEntries() const { return buf_idx_; }

  // Return the pointer buffer used for temporary copies
  byte *GetTempBuffer() const { return temp_buf_; }

 private:
  // Size of entries
  const uint64_t entry_size_;

  // Buffer space for entries
  byte buffer_[BUFFER_SIZE_IN_BYTES];

  // The index into the buffer where the next element is written
  uint64_t buf_idx_;

  // The maximum number of elements to buffer
  const uint64_t max_elems_;

  // A pointer to the last entry slot in the buffer space; used for costly swaps
  byte *const temp_buf_;

  // The index of the next element to read from the entries list
  uint64_t read_idx_;

  // The exclusive upper bound index to read from the entries list
  const uint64_t end_read_idx_;

  // Source of all entries
  util::ChunkedVector<MemoryPoolAllocator<byte>> *entries_;
};

}  // namespace

template <bool PrefetchCHT, bool PrefetchEntries>
void JoinHashTable::ReorderMainEntries() {
  const uint64_t elem_size = entries_.ElementSize();
  const uint64_t num_overflow_entries = concise_hash_table_.GetOverflowEntryCount();
  const uint64_t num_main_entries = entries_.size() - num_overflow_entries;
  uint64_t overflow_idx = num_main_entries;

  if (num_main_entries == 0) {
    return;
  }

  // This function reorders entries in-place in the main arena according to the
  // order it should appear in the concise hash table. This function uses a
  // order buffer, a temporary workspace, to perform the reordering. The
  // procedure is:
  // 1. Buffer N tuples. These can be either main or overflow entries.
  // 2. Find and store their destination addresses in the 'targets' buffer
  // 3. Try and store the buffered entry into discovered target space from (2)
  //    There are a few cases we handle:
  //    3a. If the target entry is 'PROCESSED', then the buffered entry is an
  //        overflow entry. We acquire an overflow slot and store the buffered
  //        entry there.
  //    3b. If the target entry is 'BUFFERED', it is either stored somewhere in
  //        the reorder buffer, or has been stored into its own target space.
  //        Either way, we can directly overwrite the target with the buffered
  //        entry and be done with it.
  //    3c. We need to swap the target and buffer entry. If the reorder buffer
  //        has room, copy the target into the buffer and write the buffered
  //        entry out.
  //    3d. If the reorder buffer has no room for this target entry, we use a
  //        temporary space to perform a (costly) three-way swap to buffer the
  //        target entry into the reorder buffer and write the buffered entry
  //        out. This should happen infrequently.
  // 4. Reset the reorder buffer and repeat.

  HashTableEntry *targets[common::Constants::K_DEFAULT_VECTOR_SIZE];
  ReorderBuffer reorder_buf(&entries_, common::Constants::K_DEFAULT_VECTOR_SIZE, 0, overflow_idx);

  while (reorder_buf.Fill()) {
    const uint64_t num_buf_entries = reorder_buf.GetNumEntries();

    for (uint64_t idx = 0, prefetch_idx = idx + common::Constants::K_PREFETCH_DISTANCE; idx < num_buf_entries;
         idx++, prefetch_idx++) {
      if constexpr (PrefetchCHT) {  // NOLINT
        if (LIKELY(prefetch_idx < num_buf_entries)) {
          auto *pf_entry = reorder_buf.BufEntryAt<HashTableEntry>(prefetch_idx);
          concise_hash_table_.PrefetchSlotGroup<true>(pf_entry->hash_);
        }
      }

      const auto *const entry = reorder_buf.BufEntryAt<HashTableEntry>(idx);
      uint64_t dest_idx = concise_hash_table_.NumFilledSlotsBefore(entry->cht_slot_);
      targets[idx] = EntryAt(dest_idx);
    }

    uint64_t buf_write_idx = 0;
    for (uint64_t idx = 0, prefetch_idx = idx + common::Constants::K_PREFETCH_DISTANCE; idx < num_buf_entries;
         idx++, prefetch_idx++) {
      if constexpr (PrefetchEntries) {  // NOLINT
        if (LIKELY(prefetch_idx < num_buf_entries)) {
          util::Memory::Prefetch<false, Locality::Low>(targets[prefetch_idx]);
        }
      }

      // Where we'll try to write the buffered entry
      HashTableEntry *dest = targets[idx];

      // The buffered entry we're looking at
      byte *const buf_entry = reorder_buf.BufEntryAt(idx);

      if (reorder_buf.IsProcessed(dest)) {
        dest = EntryAt(overflow_idx++);
      } else {
        reorder_buf.SetProcessed(reinterpret_cast<HashTableEntry *>(buf_entry));
      }

      if (reorder_buf.IsBuffered(dest)) {
        std::memcpy(static_cast<void *>(dest), buf_entry, elem_size);
      } else if (buf_write_idx < idx) {
        std::memcpy(reorder_buf.BufEntryAt(buf_write_idx++), static_cast<void *>(dest), elem_size);
        std::memcpy(static_cast<void *>(dest), buf_entry, elem_size);
      } else {
        byte *const tmp = reorder_buf.GetTempBuffer();
        std::memcpy(tmp, static_cast<void *>(dest), elem_size);
        std::memcpy(static_cast<void *>(dest), buf_entry, elem_size);
        std::memcpy(reorder_buf.BufEntryAt(buf_write_idx++), tmp, elem_size);
      }
    }

    reorder_buf.Reset(buf_write_idx);
  }
}

template <bool PrefetchCHT, bool PrefetchEntries>
void JoinHashTable::ReorderOverflowEntries() {
  const uint64_t elem_size = entries_.ElementSize();
  const uint64_t num_entries = entries_.size();
  const uint64_t num_overflow_entries = concise_hash_table_.GetOverflowEntryCount();
  const uint64_t num_main_entries = num_entries - num_overflow_entries;
  const uint64_t overflow_start_idx = num_main_entries;
  const uint64_t no_overflow = std::numeric_limits<uint64_t>::max();

  // General idea:
  // -------------
  // This function reorders the overflow entries in-place. The high-level idea
  // is to reorder the entries stored in the overflow area so that probe chains
  // are stored contiguously. We also need to hook up the 'next' entries from
  // the main arena to the overflow arena.
  //
  // Step 1:
  // -------
  // For each main entry, we maintain an "overflow count" which also acts as an
  // index in the entries array where overflow elements for the entry belongs.
  // We begin by clearing these counts (which were modified earlier when
  // rearranging the main entries).

  for (uint64_t idx = 0; idx < num_main_entries; idx++) {
    EntryAt(idx)->overflow_count_ = 0;
  }

  // If there arne't any overflow entries, we can early exit. We just NULLed
  // overflow pointers in all main arena entries in the loop above.

  if (num_overflow_entries == 0) {
    return;
  }

  // Step 2:
  // -------
  // Now iterate over the overflow entries (in vectors) and update the overflow
  // count for each overflow entry's parent. At the end of this process if a
  // main arena entry has an overflow chain the "overflow count" will count the
  // length of the chain.

  HashTableEntry *parents[common::Constants::K_DEFAULT_VECTOR_SIZE];

  for (uint64_t start = overflow_start_idx; start < num_entries; start += common::Constants::K_DEFAULT_VECTOR_SIZE) {
    const uint64_t vec_size = std::min(uint64_t{common::Constants::K_DEFAULT_VECTOR_SIZE}, num_entries - start);
    const uint64_t end = start + vec_size;

    for (uint64_t idx = start, write_idx = 0, prefetch_idx = idx + common::Constants::K_PREFETCH_DISTANCE; idx < end;
         idx++, write_idx++, prefetch_idx++) {
      if constexpr (PrefetchCHT) {  // NOLINT
        if (LIKELY(prefetch_idx < end)) {
          HashTableEntry *prefetch_entry = EntryAt(prefetch_idx);
          concise_hash_table_.NumFilledSlotsBefore(prefetch_entry->cht_slot_);
        }
      }

      HashTableEntry *entry = EntryAt(idx);
      uint64_t chain_idx = concise_hash_table_.NumFilledSlotsBefore(entry->cht_slot_);
      parents[write_idx] = EntryAt(chain_idx);
    }

    for (uint64_t idx = 0; idx < vec_size; idx++) {
      parents[idx]->overflow_count_++;
    }
  }

  // Step 3:
  // -------
  // Now iterate over all main arena entries and compute a prefix sum of the
  // overflow counts. At the end of this process, if a main entry has an
  // overflow, the "overflow count" will be one greater than the index of the
  // last overflow entry in the overflow chain. We use these indexes in the last
  // step when assigning overflow entries to their final locations.

  for (uint64_t idx = 0, count = 0; idx < num_main_entries; idx++) {
    HashTableEntry *entry = EntryAt(idx);
    count += entry->overflow_count_;
    entry->overflow_count_ = (entry->overflow_count_ == 0 ? no_overflow : num_main_entries + count);
  }

  // Step 4:
  // ------
  // Buffer N overflow entries into a reorder buffer and find each's main arena
  // parent. Use the "overflow count" in the main arena entry as the destination
  // index to write the overflow entry into.

  ReorderBuffer reorder_buf(&entries_, common::Constants::K_DEFAULT_VECTOR_SIZE, overflow_start_idx, num_entries);
  while (reorder_buf.Fill()) {
    const uint64_t num_buf_entries = reorder_buf.GetNumEntries();

    // For each overflow entry, find its main entry parent in the overflow chain
    for (uint64_t idx = 0, prefetch_idx = idx + common::Constants::K_PREFETCH_DISTANCE; idx < num_buf_entries;
         idx++, prefetch_idx++) {
      if constexpr (PrefetchCHT) {  // NOLINT
        if (LIKELY(prefetch_idx < num_buf_entries)) {
          auto *pf_entry = reorder_buf.BufEntryAt<HashTableEntry>(prefetch_idx);
          concise_hash_table_.PrefetchSlotGroup<true>(pf_entry->hash_);
        }
      }

      auto *entry = reorder_buf.BufEntryAt<HashTableEntry>(idx);
      uint64_t dest_idx = concise_hash_table_.NumFilledSlotsBefore(entry->cht_slot_);
      parents[idx] = EntryAt(dest_idx);
    }

    // For each overflow entry, look at the overflow count in its main parent
    // to acquire a slot in the overflow arena.
    uint64_t buf_write_idx = 0;
    for (uint64_t idx = 0, prefetch_idx = idx + common::Constants::K_PREFETCH_DISTANCE; idx < num_buf_entries;
         idx++, prefetch_idx++) {
      if constexpr (PrefetchEntries) {  // NOLINT
        if (LIKELY(prefetch_idx < num_buf_entries)) {
          util::Memory::Prefetch<false, Locality::Low>(parents[prefetch_idx]);
        }
      }

      // The buffered entry we're going to process
      byte *const buf_entry = reorder_buf.BufEntryAt(idx);
      reorder_buf.SetProcessed(reinterpret_cast<HashTableEntry *>(buf_entry));

      // Where we'll try to write the buffered entry
      HashTableEntry *target = EntryAt(--parents[idx]->overflow_count_);

      if (reorder_buf.IsBuffered(target)) {
        std::memcpy(static_cast<void *>(target), buf_entry, elem_size);
      } else if (buf_write_idx < idx) {
        std::memcpy(reorder_buf.BufEntryAt(buf_write_idx++), static_cast<void *>(target), elem_size);
        std::memcpy(static_cast<void *>(target), buf_entry, elem_size);
      } else {
        byte *const tmp = reorder_buf.GetTempBuffer();
        std::memcpy(tmp, static_cast<void *>(target), elem_size);
        std::memcpy(static_cast<void *>(target), buf_entry, elem_size);
        std::memcpy(reorder_buf.BufEntryAt(buf_write_idx++), tmp, elem_size);
      }
    }

    reorder_buf.Reset(buf_write_idx);
  }

  // Step 5:
  // -------
  // Final chain hookup. We decompose this into two parts: one loop for the main
  // arena entries and one loop for the overflow entries.
  //
  // If an entry in the main arena has an overflow chain, the "overflow count"
  // field will contain the index of the next entry in the overflow chain in
  // the overflow arena.
  //
  // For the overflow entries, we connect all overflow entries mapping to the
  // same CHT slot. At this point, entries with the same CHT slot are guaranteed
  // to be store contiguously.

  for (uint64_t idx = 0; idx < num_main_entries; idx++) {
    HashTableEntry *entry = EntryAt(idx);
    const bool has_overflow = (entry->overflow_count_ != no_overflow);
    entry->next_ = (has_overflow ? EntryAt(entry->overflow_count_) : nullptr);
  }

  for (uint64_t idx = overflow_start_idx + 1; idx < num_entries; idx++) {
    HashTableEntry *prev = EntryAt(idx - 1);
    HashTableEntry *curr = EntryAt(idx);
    prev->next_ = (prev->cht_slot_ == curr->cht_slot_ ? curr : nullptr);
  }

  // Don't forget the last gal
  EntryAt(num_entries - 1)->next_ = nullptr;
}

void JoinHashTable::VerifyMainEntryOrder() {
#ifndef NDEBUG
  constexpr const uint64_t cht_slot_mask = BUFFERED_BIT - 1;

  const uint64_t overflow_idx = entries_.size() - concise_hash_table_.GetOverflowEntryCount();
  for (uint32_t idx = 0; idx < overflow_idx; idx++) {
    auto *entry = reinterpret_cast<HashTableEntry *>(entries_[idx]);
    auto dest_idx = concise_hash_table_.NumFilledSlotsBefore(entry->cht_slot_ & cht_slot_mask);
    if (idx != dest_idx) {
      EXECUTION_LOG_ERROR("Entry {} has CHT slot {}. Found @ {}, but should be @ {}", static_cast<void *>(entry),
                          entry->cht_slot_, idx, dest_idx);
    }
  }
#endif
}

void JoinHashTable::VerifyOverflowEntryOrder() {
#ifndef NDEBUG
#endif
}

template <bool PrefetchCHT, bool PrefetchEntries>
void JoinHashTable::BuildConciseHashTableInternal() {
  // Bulk-load all buffered tuples, then build
  concise_hash_table_.InsertBatch(&entries_);
  concise_hash_table_.Build();

  EXECUTION_LOG_TRACE("Concise Table Stats: {} entries, {} overflow ({} % overflow)", entries_.size(),
                      concise_hash_table_.GetOverflowEntryCount(),
                      100.0 * (concise_hash_table_.GetOverflowEntryCount() * 1.0 / entries_.size()));

  // Reorder all the main entries in place according to CHT order
  ReorderMainEntries<PrefetchCHT, PrefetchEntries>();

  // Verify (no-op in debug)
  VerifyMainEntryOrder();

  // Reorder all the overflow entries in place according to CHT order
  ReorderOverflowEntries<PrefetchCHT, PrefetchEntries>();

  // Verify (no-op in debug)
  VerifyOverflowEntryOrder();
}

void JoinHashTable::BuildConciseHashTable() {
  // Perfectly size the concise hash table in preparation for bulk-load.
  concise_hash_table_.SetSize(GetTupleCount(), tracker_);

  // Dispatch to internal function based on prefetching requirements. If the CHT
  // is larger than L3 then the total size of all buffered build-side tuples is
  // also larger than L3; in this case prefetch from both when building the CHT.
  // If the CHT fits in cache, it's still possible that build tuples do not.

  uint64_t l3_cache_size = CpuInfo::Instance()->GetCacheSize(CpuInfo::L3_CACHE);
  if (concise_hash_table_.GetTotalMemoryUsage() > l3_cache_size) {
    BuildConciseHashTableInternal<true, true>();
  } else if (GetBufferedTupleMemoryUsage() > l3_cache_size) {
    BuildConciseHashTableInternal<false, true>();
  } else {
    BuildConciseHashTableInternal<false, false>();
  }
}

void JoinHashTable::Build() {
  if (IsBuilt()) {
    return;
  }

  EXECUTION_LOG_DEBUG("Unique estimate: {}", hll_estimator_->Estimate());

  util::Timer<> timer;
  timer.Start();

  // Build
  if (UsingConciseHashTable()) {
    BuildConciseHashTable();
  } else {
    BuildChainingHashTable();
  }

  timer.Stop();
  UNUSED_ATTRIBUTE double tps = (GetTupleCount() / timer.GetElapsed()) / 1000.0;
  EXECUTION_LOG_DEBUG("JHT: built {} tuples in {} ms ({:.2f} tps)", GetTupleCount(), timer.GetElapsed(), tps);

  built_ = true;
}

// TODO(pmenon): Vectorized bloom filter pre-filtering.
// TODO(pmenon): Implement prefetching.

void JoinHashTable::LookupBatchInChainingHashTable(const Vector &hashes, Vector *results) const {
  UnaryOperationExecutor::Execute<hash_t, const HashTableEntry *>(
      exec_settings_, hashes,
      results, [&](const hash_t hash_val) noexcept { return chaining_hash_table_.FindChainHead(hash_val); });
}

void JoinHashTable::LookupBatchInConciseHashTable(const Vector &hashes, Vector *results) const {
  UnaryOperationExecutor::Execute<hash_t, const HashTableEntry *>(
      exec_settings_, hashes, results, [&](const hash_t hash_val) noexcept {
        const auto [found, entry_idx] = concise_hash_table_.Lookup(hash_val);
        return (found ? EntryAt(entry_idx) : nullptr);
      });
}

void JoinHashTable::LookupBatch(const Vector &hashes, Vector *results) const {
  TERRIER_ASSERT(IsBuilt(), "Cannot perform lookup before table is built!");
  if (UsingConciseHashTable()) {
    LookupBatchInConciseHashTable(hashes, results);
  } else {
    LookupBatchInChainingHashTable(hashes, results);
  }
}

template <bool Concurrent>
void JoinHashTable::MergeIncomplete(JoinHashTable *source) {
  // TODO(pmenon): Support merging build of concise tables
  TERRIER_ASSERT(!source->UsingConciseHashTable(), "Merging incomplete concise tables not supported");

  // First, bulk-load all entries in the source table into our hash table
  chaining_hash_table_.InsertBatch<Concurrent>(&source->entries_);

  // Next, take ownership of source table's memory
  common::SpinLatch::ScopedSpinLatch latch(&owned_latch_);
  owned_.emplace_back(std::move(source->entries_));
}

void JoinHashTable::MergeParallel(ThreadStateContainer *thread_state_container, const std::size_t jht_offset) {
  // Collect thread-local hash tables
  std::vector<JoinHashTable *> tl_join_tables;
  thread_state_container->CollectThreadLocalStateElementsAs(&tl_join_tables, jht_offset);

  // Combine HLL counts to get a global estimate
  for (auto *jht : tl_join_tables) {
    hll_estimator_->Merge(jht->hll_estimator_.get());
  }

  // Size the global hash table
  uint64_t num_elem_estimate = hll_estimator_->Estimate();
  chaining_hash_table_.SetSize(num_elem_estimate, tracker_);

  // Resize the owned entries vector now to avoid resizing concurrently during
  // merge. All the thread-local join table data will get placed into our owned
  // entries vector.
  owned_.reserve(tl_join_tables.size());

  util::Timer<std::milli> timer;
  timer.Start();

  const bool use_serial_build = num_elem_estimate < DEFAULT_MIN_SIZE_FOR_PARALLEL_MERGE;
  if (use_serial_build) {
    // TODO(pmenon): Switch to parallel-mode if estimate is wrong.
    EXECUTION_LOG_TRACE("JHT: Estimated {} elements < {} element parallel threshold. Using serial merge.",
                        num_elem_estimate, DEFAULT_MIN_SIZE_FOR_PARALLEL_MERGE);

    auto pre_hook = static_cast<uint32_t>(HookOffsets::StartHook);
    auto post_hook = static_cast<uint32_t>(HookOffsets::EndHook);
    auto *tls = thread_state_container->AccessCurrentThreadState();
    exec_ctx_->InvokeHook(pre_hook, tls, nullptr);

    llvm::for_each(tl_join_tables, [this](auto *source) { MergeIncomplete<false>(source); });

    exec_ctx_->InvokeHook(post_hook, tls, reinterpret_cast<void *>(num_elem_estimate));
  } else {
    EXECUTION_LOG_TRACE("JHT: Estimated {} elements >= {} element parallel threshold. Using parallel merge.",
                        num_elem_estimate, DEFAULT_MIN_SIZE_FOR_PARALLEL_MERGE);

    size_t num_threads = tbb::task_scheduler_init::default_num_threads();
    size_t num_tasks = tl_join_tables.size();
    auto estimate = std::min(num_threads, num_tasks);
    exec_ctx_->SetNumConcurrentEstimate(estimate);
    tbb::parallel_for_each(tl_join_tables, [this, thread_state_container](auto source) {
      auto pre_hook = static_cast<uint32_t>(HookOffsets::StartHook);
      auto post_hook = static_cast<uint32_t>(HookOffsets::EndHook);
      auto *tls = thread_state_container->AccessCurrentThreadState();
      exec_ctx_->InvokeHook(pre_hook, tls, nullptr);

      size_t size = source->entries_.size();
      MergeIncomplete<true>(source);
      exec_ctx_->InvokeHook(post_hook, tls, reinterpret_cast<void *>(size));
    });
    exec_ctx_->SetNumConcurrentEstimate(0);
  }

  timer.Stop();

  UNUSED_ATTRIBUTE const double tps = (chaining_hash_table_.GetElementCount() / timer.GetElapsed()) / 1000.0;
  EXECUTION_LOG_TRACE("JHT: {} merged {} JHTs. Estimated {}, actual {}. Time: {:.2f} ms ({:.2f} mtps)",
                      use_serial_build ? "Serial" : "Parallel", tl_join_tables.size(), num_elem_estimate,
                      chaining_hash_table_.GetElementCount(), timer.GetElapsed(), tps);

  built_ = true;
}

}  // namespace terrier::execution::sql
