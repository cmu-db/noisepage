#include "execution/sql/join_hash_table.h"

#include <algorithm>
#include <limits>
#include <utility>
#include <vector>

#include "tbb/tbb.h"

#include "libcount/hll.h"

#include "execution/sql/memory_pool.h"
#include "execution/sql/thread_state_container.h"
#include "execution/util/cpu_info.h"
#include "execution/util/memory.h"
#include "execution/util/timer.h"
#include "loggers/execution_logger.h"

namespace terrier::execution::sql {

JoinHashTable::JoinHashTable(MemoryPool *memory, uint32_t tuple_size, bool use_concise_ht)
    : entries_(sizeof(HashTableEntry) + tuple_size, MemoryPoolAllocator<byte>(memory)),
      owned_(memory),
      concise_hash_table_(0),
      hll_estimator_(libcount::HLL::Create(K_DEFAULT_HLL_PRECISION)),
      built_(false),
      use_concise_ht_(use_concise_ht) {}

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

// ---------------------------------------------------------
// Generic hash tables
// ---------------------------------------------------------

template <bool Prefetch>
void JoinHashTable::BuildGenericHashTableInternal() noexcept {
  for (uint64_t idx = 0, prefetch_idx = common::Constants::K_PREFETCH_DISTANCE; idx < entries_.size();
       idx++, prefetch_idx++) {
    // NOLINTNEXTLINE: bugprone-suspicious-semicolon: seems like a false positive because of constexpr
    if constexpr (Prefetch) {
      if (LIKELY(prefetch_idx < entries_.size())) {
        auto *prefetch_entry = EntryAt(prefetch_idx);
        generic_hash_table_.PrefetchChainHead<false>(prefetch_entry->hash_);
      }
    }

    HashTableEntry *entry = EntryAt(idx);
    generic_hash_table_.Insert<false>(entry, entry->hash_);
  }
}

void JoinHashTable::BuildGenericHashTable() noexcept {
  // Setup based on number of buffered build-size tuples
  generic_hash_table_.SetSize(NumElements());

  // Dispatch to appropriate build code based on GHT size
  uint64_t l3_cache_size = CpuInfo::Instance()->GetCacheSize(CpuInfo::L3_CACHE);
  if (generic_hash_table_.GetTotalMemoryUsage() > l3_cache_size) {
    BuildGenericHashTableInternal<true>();
  } else {
    BuildGenericHashTableInternal<false>();
  }
}

// ---------------------------------------------------------
// Concise hash tables
// ---------------------------------------------------------

template <bool Prefetch>
void JoinHashTable::InsertIntoConciseHashTable() noexcept {
  for (uint64_t idx = 0, prefetch_idx = common::Constants::K_PREFETCH_DISTANCE; idx < entries_.size();
       idx++, prefetch_idx++) {
    // NOLINTNEXTLINE: bugprone-suspicious-semicolon: seems like a false positive because of constexpr
    if constexpr (Prefetch) {
      if (LIKELY(prefetch_idx < entries_.size())) {
        auto *prefetch_entry = EntryAt(prefetch_idx);
        concise_hash_table_.PrefetchSlotGroup<false>(prefetch_entry->hash_);
      }
    }

    HashTableEntry *entry = EntryAt(idx);
    concise_hash_table_.Insert(entry, entry->hash_);
  }
}

namespace {

// The bits we set in the entry to mark if the entry has been buffered in the
// reorder buffer and whether the entry has been processed (i.e., if the entry
// is in its final location in either the main or overflow arenas).
constexpr const uint64_t K_BUFFERED_BIT = 1ull << 62ull;
constexpr const uint64_t K_PROCESSED_BIT = 1ull << 63ull;

/**
 * A reorder is a small piece of buffer space into which we temporarily buffer
 * hash table entries for the purposes of reordering them.
 */
class ReorderBuffer {
 public:
  // Use a 16 common::Constants::KB internal buffer for temporary copies
  static constexpr const uint32_t K_BUFFER_SIZE_IN_BYTES = 16 * 1024;

  ReorderBuffer(util::ChunkedVector<MemoryPoolAllocator<byte>> *entries, uint64_t max_elems, uint64_t begin_read_idx,
                uint64_t end_read_idx) noexcept
      : entry_size_(entries->ElementSize()),
        buf_idx_(0),
        max_elems_(std::min(max_elems, K_BUFFER_SIZE_IN_BYTES / entry_size_) - 1),
        temp_buf_(buffer_ + (max_elems_ * entry_size_)),
        read_idx_(begin_read_idx),
        end_read_idx_(end_read_idx),
        entries_(entries) {}

  /**
   * This class cannot be copied or moved
   */
  DISALLOW_COPY_AND_MOVE(ReorderBuffer);

  /**
   * Retrieve an entry in the buffer by its index in the buffer
   * @tparam T The type to cast the resulting entry into
   * @param idx The index of the entry to lookup
   * @return A pointer to the entry
   */
  template <typename T = byte>
  T *BufEntryAt(uint64_t idx) {
    return reinterpret_cast<T *>(buffer_ + (idx * entry_size_));
  }

  /**
   * Has the entry @em been processed? In other words, is the entry in its
   * final location in the entry array?
   */
  ALWAYS_INLINE bool IsProcessed(const HashTableEntry *entry) const noexcept {
    return (entry->cht_slot_ & K_PROCESSED_BIT) != 0u;
  }

  /**
   * Mark the given entry as processed and in its final location
   */
  ALWAYS_INLINE void SetProcessed(HashTableEntry *entry) const noexcept { entry->cht_slot_ |= K_PROCESSED_BIT; }

  /**
   * Has the entry @em entry been buffered in the reorder buffer?
   */
  ALWAYS_INLINE bool IsBuffered(const HashTableEntry *entry) const noexcept {
    return (entry->cht_slot_ & K_BUFFERED_BIT) != 0u;
  }

  /**
   * Mark the entry @em entry as buffered in the reorder buffer
   */
  ALWAYS_INLINE void SetBuffered(HashTableEntry *entry) const noexcept { entry->cht_slot_ |= K_BUFFERED_BIT; }

  /**
   * Fill this reorder buffer with as many entries as possible. Each entry that
   * is inserted is marked/tagged as buffered.
   * @return True if any entries were buffered; false otherwise
   */
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

  /**
   * Reset the index where the next buffered entry goes. This is needed when,
   * in the process of
   */
  void Reset(const uint64_t new_buf_idx) noexcept { buf_idx_ = new_buf_idx; }

  // -------------------------------------------------------
  // Accessors
  // -------------------------------------------------------

  uint64_t NumEntries() const { return buf_idx_; }

  byte *TempBuffer() const { return temp_buf_; }

 private:
  // Size of entries
  const uint64_t entry_size_;

  // Buffer space for entries
  byte buffer_[K_BUFFER_SIZE_IN_BYTES];

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
void JoinHashTable::ReorderMainEntries() noexcept {
  const uint64_t elem_size = entries_.ElementSize();
  const uint64_t num_overflow_entries = concise_hash_table_.NumOverflow();
  const uint64_t num_main_entries = entries_.size() - num_overflow_entries;
  uint64_t overflow_idx = num_main_entries;

  if (num_main_entries == 0) {
    return;
  }

  //
  // This function reorders entries in-place in the main arena using a reorder
  // buffer space. The general process is:
  // 1. Buffer N tuples. These can be either main or overflow entries.
  // 2. Find and store their destination addresses in the 'targets' buffer
  // 3. Try and store the buffered entry into discovered target space from (2)
  //    There are a few cases we handle:
  //    3a. If the target entry is 'PROCESSED', then the buffered entry is an
  //        overflow entry. We acquire an overflow slot and store the buffered
  //        entry there.
  //    3b. If the target entry is 'BUFFERED', it is either stored somewhere
  //        in the reorder buffer, or has been stored into its own target space.
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
  //

  HashTableEntry *targets[common::Constants::K_DEFAULT_VECTOR_SIZE];
  ReorderBuffer reorder_buf(&entries_, common::Constants::K_DEFAULT_VECTOR_SIZE, 0, overflow_idx);

  while (reorder_buf.Fill()) {
    const uint64_t num_buf_entries = reorder_buf.NumEntries();

    for (uint64_t idx = 0, prefetch_idx = idx + common::Constants::K_PREFETCH_DISTANCE; idx < num_buf_entries;
         idx++, prefetch_idx++) {
      // NOLINTNEXTLINE: bugprone-suspicious-semicolon: seems like a false positive because of constexpr
      if constexpr (PrefetchCHT) {
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
      // NOLINTNEXTLINE: bugprone-suspicious-semicolon: seems like a false positive because of constexpr
      if constexpr (PrefetchEntries) {
        if (LIKELY(prefetch_idx < num_buf_entries)) {
          util::Prefetch<false, Locality::Low>(targets[prefetch_idx]);
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
        byte *const tmp = reorder_buf.TempBuffer();
        std::memcpy(tmp, static_cast<void *>(dest), elem_size);
        std::memcpy(static_cast<void *>(dest), buf_entry, elem_size);
        std::memcpy(reorder_buf.BufEntryAt(buf_write_idx++), tmp, elem_size);
      }
    }

    reorder_buf.Reset(buf_write_idx);
  }
}

template <bool PrefetchCHT, bool PrefetchEntries>
void JoinHashTable::ReorderOverflowEntries() noexcept {
  const uint64_t elem_size = entries_.ElementSize();
  const uint64_t num_entries = entries_.size();
  const uint64_t num_overflow_entries = concise_hash_table_.NumOverflow();
  const uint64_t num_main_entries = num_entries - num_overflow_entries;
  const uint64_t overflow_start_idx = num_main_entries;
  const uint64_t no_overflow = std::numeric_limits<uint64_t>::max();

  //
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
  //

  for (uint64_t idx = 0; idx < num_main_entries; idx++) {
    EntryAt(idx)->overflow_count_ = 0;
  }

  //
  // If there arne't any overflow entries, we can early exit. We just NULLed
  // overflow pointers in all main arena entries in the loop above.
  //

  if (num_overflow_entries == 0) {
    return;
  }

  //
  // Step 2:
  // -------
  // Now iterate over the overflow entries (in vectors) and update the overflow
  // count for each overflow entry's parent. At the end of this process if a
  // main arena entry has an overflow chain the "overflow count" will count the
  // length of the chain.
  //

  HashTableEntry *parents[common::Constants::K_DEFAULT_VECTOR_SIZE];

  for (uint64_t start = overflow_start_idx; start < num_entries; start += common::Constants::K_DEFAULT_VECTOR_SIZE) {
    const uint64_t vec_size = std::min(uint64_t{common::Constants::K_DEFAULT_VECTOR_SIZE}, num_entries - start);
    const uint64_t end = start + vec_size;

    for (uint64_t idx = start, write_idx = 0, prefetch_idx = idx + common::Constants::K_PREFETCH_DISTANCE; idx < end;
         idx++, write_idx++, prefetch_idx++) {
      // NOLINTNEXTLINE: bugprone-suspicious-semicolon: seems like a false positive because of constexpr
      if constexpr (PrefetchCHT) {
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

  //
  // Step 3:
  // -------
  // Now iterate over all main arena entries and compute a prefix sum of the
  // overflow counts. At the end of this process, if a main entry has an
  // overflow, the "overflow count" will be one greater than the index of the
  // last overflow entry in the overflow chain. We use these indexes in the last
  // step when assigning overflow entries to their final locations.
  //

  for (uint64_t idx = 0, count = 0; idx < num_main_entries; idx++) {
    HashTableEntry *entry = EntryAt(idx);
    count += entry->overflow_count_;
    entry->overflow_count_ = (entry->overflow_count_ == 0 ? no_overflow : num_main_entries + count);
  }

  //
  // Step 4:
  // ------
  // Buffer N overflow entries into a reorder buffer and find each's main arena
  // parent. Use the "overflow count" in the main arena entry as the destination
  // index to write the overflow entry into.
  //

  ReorderBuffer reorder_buf(&entries_, common::Constants::K_DEFAULT_VECTOR_SIZE, overflow_start_idx, num_entries);
  while (reorder_buf.Fill()) {
    const uint64_t num_buf_entries = reorder_buf.NumEntries();

    // For each overflow entry, find its main entry parent in the overflow chain
    for (uint64_t idx = 0, prefetch_idx = idx + common::Constants::K_PREFETCH_DISTANCE; idx < num_buf_entries;
         idx++, prefetch_idx++) {
      // NOLINTNEXTLINE: bugprone-suspicious-semicolon: seems like a false positive because of constexpr
      if constexpr (PrefetchCHT) {
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
      // NOLINTNEXTLINE: bugprone-suspicious-semicolon: seems like a false positive because of constexpr
      if constexpr (PrefetchEntries) {
        if (LIKELY(prefetch_idx < num_buf_entries)) {
          util::Prefetch<false, Locality::Low>(parents[prefetch_idx]);
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
        byte *const tmp = reorder_buf.TempBuffer();
        std::memcpy(tmp, static_cast<void *>(target), elem_size);
        std::memcpy(static_cast<void *>(target), buf_entry, elem_size);
        std::memcpy(reorder_buf.BufEntryAt(buf_write_idx++), tmp, elem_size);
      }
    }

    reorder_buf.Reset(buf_write_idx);
  }

  //
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
  //

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
  constexpr const uint64_t k_cht_slot_mask = K_BUFFERED_BIT - 1;

  const uint64_t overflow_idx = entries_.size() - concise_hash_table_.NumOverflow();
  for (uint32_t idx = 0; idx < overflow_idx; idx++) {
    auto *entry = reinterpret_cast<HashTableEntry *>(entries_[idx]);
    auto dest_idx = concise_hash_table_.NumFilledSlotsBefore(entry->cht_slot_ & k_cht_slot_mask);
    if (idx != dest_idx) {
      EXECUTION_LOG_ERROR("Entry {} has CHT slot {}. Found @ {}, but should be @ {}", static_cast<void *>(entry),
                          entry->cht_slot_, idx, dest_idx);
    }
  }
#endif
}

void JoinHashTable::VerifyOverflowEntryOrder() noexcept {
#ifndef NDEBUG
#endif
}

template <bool PrefetchCHT, bool PrefetchEntries>
void JoinHashTable::BuildConciseHashTableInternal() {
  // Insert all elements
  InsertIntoConciseHashTable<PrefetchCHT>();

  // Build
  concise_hash_table_.Build();

  EXECUTION_LOG_INFO(
      "Concise Table Stats: {} entries, {} overflow ({} % overflow)", entries_.size(),
      concise_hash_table_.NumOverflow(),
      100.0 * (static_cast<double>(concise_hash_table_.NumOverflow()) * 1.0 / static_cast<double>(entries_.size())));

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
  // Setup based on number of buffered build-size tuples
  concise_hash_table_.SetSize(static_cast<uint32_t>(NumElements()));

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
  if (UseConciseHashTable()) {
    BuildConciseHashTable();
  } else {
    BuildGenericHashTable();
  }

  timer.Stop();
  UNUSED_ATTRIBUTE double tps = (static_cast<double>(NumElements()) / timer.Elapsed()) / 1000.0;
  EXECUTION_LOG_DEBUG("JHT: built {} tuples in {} ms ({:.2f} tps)", NumElements(), timer.Elapsed(), tps);

  built_ = true;
}

template <bool Prefetch>
void JoinHashTable::LookupBatchInGenericHashTableInternal(uint32_t num_tuples, const hash_t hashes[],
                                                          const HashTableEntry *results[]) const {
  // TODO(pmenon): Use tagged insertions/probes if no bloom filter exists

  // Initial lookup
  for (uint32_t idx = 0, prefetch_idx = common::Constants::K_PREFETCH_DISTANCE; idx < num_tuples;
       idx++, prefetch_idx++) {
    // NOLINTNEXTLINE: bugprone-suspicious-semicolon: seems like a false positive because of constexpr
    if constexpr (Prefetch) {
      if (LIKELY(prefetch_idx < num_tuples)) {
        generic_hash_table_.PrefetchChainHead<true>(hashes[prefetch_idx]);
      }
    }

    results[idx] = generic_hash_table_.FindChainHead(hashes[idx]);
  }
}

void JoinHashTable::LookupBatchInGenericHashTable(uint32_t num_tuples, const hash_t hashes[],
                                                  const HashTableEntry *results[]) const {
  uint64_t l3_cache_size = CpuInfo::Instance()->GetCacheSize(CpuInfo::L3_CACHE);
  if (generic_hash_table_.GetTotalMemoryUsage() > l3_cache_size) {
    LookupBatchInGenericHashTableInternal<true>(num_tuples, hashes, results);
  } else {
    LookupBatchInGenericHashTableInternal<false>(num_tuples, hashes, results);
  }
}

template <bool Prefetch>
void JoinHashTable::LookupBatchInConciseHashTableInternal(uint32_t num_tuples, const hash_t hashes[],
                                                          const HashTableEntry *results[]) const {
  for (uint32_t idx = 0, prefetch_idx = common::Constants::K_PREFETCH_DISTANCE; idx < num_tuples;
       idx++, prefetch_idx++) {
    // NOLINTNEXTLINE: bugprone-suspicious-semicolon: seems like a false positive because of constexpr
    if constexpr (Prefetch) {
      if (LIKELY(prefetch_idx < num_tuples)) {
        concise_hash_table_.PrefetchSlotGroup<true>(hashes[prefetch_idx]);
      }
    }
    // NOLINTNEXTLINE
    const auto [found, entry_idx] = concise_hash_table_.Lookup(hashes[idx]);
    results[idx] = (found ? EntryAt(entry_idx) : nullptr);
  }
}

void JoinHashTable::LookupBatchInConciseHashTable(uint32_t num_tuples, const hash_t hashes[],
                                                  const HashTableEntry *results[]) const {
  uint64_t l3_cache_size = CpuInfo::Instance()->GetCacheSize(CpuInfo::L3_CACHE);
  if (concise_hash_table_.GetTotalMemoryUsage() > l3_cache_size) {
    LookupBatchInConciseHashTableInternal<true>(num_tuples, hashes, results);
  } else {
    LookupBatchInConciseHashTableInternal<false>(num_tuples, hashes, results);
  }
}

void JoinHashTable::LookupBatch(uint32_t num_tuples, const hash_t hashes[], const HashTableEntry *results[]) const {
  TERRIER_ASSERT(IsBuilt(), "Cannot perform lookup before table is built!");

  if (UseConciseHashTable()) {
    LookupBatchInConciseHashTable(num_tuples, hashes, results);
  } else {
    LookupBatchInGenericHashTable(num_tuples, hashes, results);
  }
}

template <bool Prefetch, bool Concurrent>
void JoinHashTable::MergeIncomplete(JoinHashTable *source) {
  // Only generic table merges are supported
  // TODO(pmenon): Support merging build of concise tables

  // First, merge entries in the source table into ours
  for (uint64_t idx = 0, prefetch_idx = common::Constants::K_PREFETCH_DISTANCE; idx < source->NumElements();
       idx++, prefetch_idx++) {
    // NOLINTNEXTLINE: bugprone-suspicious-semicolon: seems like a false positive because of constexpr
    if constexpr (Prefetch) {
      if (LIKELY(prefetch_idx < source->NumElements())) {
        auto *prefetch_entry = source->EntryAt(prefetch_idx);
        generic_hash_table_.PrefetchChainHead<false>(prefetch_entry->hash_);
      }
    }

    HashTableEntry *entry = source->EntryAt(idx);
    generic_hash_table_.Insert<Concurrent>(entry, entry->hash_);
  }

  // Next, take ownership of source table's memory
  {
    common::SpinLatch::ScopedSpinLatch latch(&owned_latch_);
    owned_.emplace_back(std::move(source->entries_));
  }
}

void JoinHashTable::MergeParallel(const ThreadStateContainer *thread_state_container, const uint32_t jht_offset) {
  // Collect thread-local hash tables
  std::vector<JoinHashTable *> tl_join_tables;
  thread_state_container->CollectThreadLocalStateElementsAs(&tl_join_tables, jht_offset);

  // Combine HLL counts to get a global estimate
  for (auto *jht : tl_join_tables) {
    hll_estimator_->Merge(jht->hll_estimator_.get());
  }

  uint64_t num_elem_estimate = hll_estimator_->Estimate();
  EXECUTION_LOG_INFO("Global unique count: {}", num_elem_estimate);

  // Set size
  generic_hash_table_.SetSize(num_elem_estimate);

  // Resize the owned entries vector now to avoid resizing concurrently during
  // merge. All the thread-local join table data will get placed into our
  // owned entries vector
  owned_.reserve(tl_join_tables.size());

  // Is the global hash table out of cache? If so, we'll prefetch during build.
  const uint64_t l3_size = CpuInfo::Instance()->GetCacheSize(CpuInfo::L3_CACHE);
  const bool out_of_cache = (generic_hash_table_.GetTotalMemoryUsage() > l3_size);

  // Merge all in parallel
  tbb::task_scheduler_init sched;
  tbb::parallel_for_each(tl_join_tables.begin(), tl_join_tables.end(), [this, out_of_cache](JoinHashTable *source) {
    if (out_of_cache) {
      MergeIncomplete<true, true>(source);
    } else {
      MergeIncomplete<false, true>(source);
    }
  });
}

}  // namespace terrier::execution::sql
