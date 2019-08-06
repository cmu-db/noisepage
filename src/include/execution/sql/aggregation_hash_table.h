#pragma once

#include <functional>

#include "execution/sql/generic_hash_table.h"
#include "execution/sql/memory_pool.h"
#include "execution/sql/projected_columns_iterator.h"
#include "execution/util/chunked_vector.h"

namespace libcount {
class HLL;
}  // namespace libcount

namespace terrier::sql {

class ThreadStateContainer;

// Forward declare
class AggregationHashTableIterator;
class AggregationOverflowPartitionIterator;

/**
 * The hash table used when performing aggregations
 */
class AggregationHashTable {
 public:
  /**
   * Default load factor
   */
  static constexpr const float kDefaultLoadFactor = 0.7f;

  /**
   * Default initial size
   */
  static constexpr const u32 kDefaultInitialTableSize = 256;

  /**
   * Default number of partitions
   */
  static constexpr const u32 kDefaultNumPartitions = 512;

  /**
   * Default libcount precision
   */
  static constexpr u32 kDefaultHLLPrecision = 10;

  // -------------------------------------------------------
  // Callback functions to customize aggregations
  // -------------------------------------------------------

  /**
   * Function to check the key equality of an input tuple and an existing entry
   * in the aggregation hash table.
   * Convention: First argument is the aggregate entry, second argument is the
   *             input tuple.
   */
  using KeyEqFn = bool (*)(const void *, const void *);

  /**
   * Function that takes an input element and computes a hash value.
   */
  using HashFn = hash_t (*)(void *);

  /**
   * Function to initialize a new aggregate.
   * Convention: First argument is the aggregate to initialize, second argument
   *             is the input tuple to initialize the aggregate with.
   */
  using InitAggFn = void (*)(void *, void *);

  /**
   * Function to advance an existing aggregate with a new input value.
   * Convention: First argument is the existing aggregate to update, second
   *             argument is the input tuple to update the aggregate with.
   */
  using AdvanceAggFn = void (*)(void *, void *);

  /**
   * Function to merge a set of overflow partitions into the given aggregation
   * hash table.
   * Convention: First argument is an opaque state object that the user
   *             provides. The second argument is the aggregation to be built.
   *             The third argument is the list of overflow partitions pointers,
   *             and the fourth and fifth argument are the range of overflow
   *             partitions to merge into the input aggregation hash table.
   */
  using MergePartitionFn = void (*)(void *, AggregationHashTable *, AggregationOverflowPartitionIterator *);

  /**
   * Function to scan an aggregation hash table.
   * Convention: First argument is query state, second argument is thread-local
   *             state, last argument is the aggregation hash table to scan.
   */
  using ScanPartitionFn = void (*)(void *, void *, const AggregationHashTable *);

  /**
   * Small class to capture various usage stats
   */
  struct Stats {
    /**
     * Number of hash table growth
     */
    u64 num_growths = 0;

    /**
     * Number of flushes
     */
    u64 num_flushes = 0;
  };

  // -------------------------------------------------------
  // Main API
  // -------------------------------------------------------

  /**
   * Construct an aggregation hash table using the provided memory pool, and
   * configured to store aggregates of size @em payload_size in bytes
   * @param memory The memory pool to allocate memory from
   * @param payload_size The size of the elements in the hash table
   */
  AggregationHashTable(MemoryPool *memory, std::size_t payload_size);

  /**
   * Construct an aggregation hash table using the provided memory pool,
   * configured to store aggregates of size @em payload_size in bytes, and whose
   * initial size allows for @em initial_size aggregates.
   * @param memory The memory pool to allocate memory from
   * @param payload_size The size of the elements in the hash table
   * @param initial_size The initial number of aggregates to support.
   */
  AggregationHashTable(MemoryPool *memory, std::size_t payload_size, u32 initial_size);

  /**
   * This class cannot be copied or moved
   */
  DISALLOW_COPY_AND_MOVE(AggregationHashTable);

  /**
   * Destructor
   */
  ~AggregationHashTable();

  /**
   * Insert a new element with hash value @em hash into the aggregation table.
   * @param hash The hash value of the element to insert
   * @return A pointer to a memory area where the element can be written to
   */
  byte *Insert(hash_t hash);

  /**
   * Insert a new element with hash value @em hash into this partitioned
   * aggregation hash table.
   * @param hash The hash value of the element to insert
   * @return A pointer to a memory area where the input element can be written
   */
  byte *InsertPartitioned(hash_t hash);

  /**
   * Lookup and return an entry in the aggregation table that matches a given
   * hash and key. The hash value is provided here, keys are checked using the
   * provided callback function.
   * @param hash The hash value to use for early filtering
   * @param key_eq_fn The key-equality function to resolve hash collisions
   * @param probe_tuple The probe tuple
   * @return A pointer to the matching entry payload; null if no entry is found.
   */
  byte *Lookup(hash_t hash, KeyEqFn key_eq_fn, const void *probe_tuple);

  /**
   * Process an entire vector of input.
   * @param iters The input vectors
   * @param hash_fn Function to compute a hash of an input element
   * @param key_eq_fn Function to determine key equality of an input element and
   *                  an existing aggregate
   * @param init_agg_fn Function to initialize a new aggregate
   * @param advance_agg_fn Function to advance an existing aggregate
   */
  void ProcessBatch(ProjectedColumnsIterator *iters[], HashFn hash_fn, KeyEqFn key_eq_fn, InitAggFn init_agg_fn,
                    AdvanceAggFn advance_agg_fn);

  /**
   * Transfer all entries and overflow partitions stored in each thread-local
   * aggregation hash table (in the thread state container) into this table.
   *
   * This function only moves memory around, no aggregation hash tables are
   * built. It is used at the end of the build-portion of a parallel aggregation
   * before the thread state container is reset for the next pipeline's thread-
   * local state
   *
   * @param thread_states Container for all thread-local tables.
   * @param agg_ht_offset The offset in the container to find the table.
   * @param merge_partition_fn The partition merge function
   */
  void TransferMemoryAndPartitions(ThreadStateContainer *thread_states, std::size_t agg_ht_offset,
                                   MergePartitionFn merge_partition_fn);

  /**
   * Execute a parallel scan over this partitioned hash table. It is assumed
   * that this aggregation table was constructed in a partitioned manner. This
   * function will build a new aggregation hash table for any non-empty overflow
   * partition, merge the contents of the partition (using the merging function
   * provided to the call to @em TransferMemoryAndPartitions()), and invoke the
   * scan callback function to scan the newly built table. The building and
   * scanning of the table will be performed entirely in parallel; hence, the
   * callback function should be thread-safe.
   *
   * The thread states container is assumed to already have been configured
   * prior to this scan call.
   *
   * The callback scan function accepts two opaque state objects: an query state
   * and a thread state. The query state is provided as a function argument. The
   * thread state will be pulled from the provided ThreadStateContainer object.
   *
   * @param query_state The (opaque) query state.
   * @param thread_states The container holding all thread states.
   * @param scan_fn The callback scan function that will scan one partition of
   *                the partitioned aggregation hash table.
   */
  void ExecuteParallelPartitionedScan(void *query_state, ThreadStateContainer *thread_states, ScanPartitionFn scan_fn);

  /**
   * How many aggregates are in this table?
   */
  u64 NumElements() const { return hash_table_.num_elements(); }

  /**
   * Read-only access to hash table stats
   */
  const Stats *stats() const { return &stats_; }

 private:
  friend class AggregationHashTableIterator;

  // Does the hash table need to grow?
  bool NeedsToGrow() const { return hash_table_.num_elements() >= max_fill_; }

  // Grow the hash table
  void Grow();

  // Lookup a hash table entry internally
  HashTableEntry *LookupEntryInternal(hash_t hash, KeyEqFn key_eq_fn, const void *probe_tuple) const;

  // Flush all entries currently stored in the hash table into the overflow
  // partitions
  void FlushToOverflowPartitions();

  // Allocate all overflow partition information if unallocated
  void AllocateOverflowPartitions();

  // Compute the hash value and perform the table lookup for all elements in the
  // input vector projections.
  template <bool PCIIsFiltered>
  void ProcessBatchImpl(ProjectedColumnsIterator *iters[], u32 num_elems, hash_t hashes[], HashTableEntry *entries[],
                        HashFn hash_fn, KeyEqFn key_eq_fn, InitAggFn init_agg_fn, AdvanceAggFn advance_agg_fn);

  // Called from ProcessBatch() to lookup a batch of entries. When the function
  // returns, the hashes vector will contain the hash values of all elements in
  // the input vector, and entries will contain a pointer to the associated
  // element's group aggregate, or null if no group exists.
  template <bool PCIIsFiltered>
  void LookupBatch(ProjectedColumnsIterator *iters[], u32 num_elems, hash_t hashes[], HashTableEntry *entries[],
                   HashFn hash_fn, KeyEqFn key_eq_fn) const;

  // Called from LookupBatch() to compute and fill the hashes input vector with
  // the hash values of all input tuples, and to load the initial set of
  // candidate groups into the entries vector. When this function returns, the
  // hashes vector will be full, and the entries vector will contain either a
  // pointer to the first (of potentially many) elements in the hash table that
  // match the input hash value.
  template <bool PCIIsFiltered>
  void ComputeHashAndLoadInitial(ProjectedColumnsIterator *iters[], u32 num_elems, hash_t hashes[],
                                 HashTableEntry *entries[], HashFn hash_fn) const;
  template <bool PCIIsFiltered, bool Prefetch>
  void ComputeHashAndLoadInitialImpl(ProjectedColumnsIterator *iters[], u32 num_elems, hash_t hashes[],
                                     HashTableEntry *entries[], HashFn hash_fn) const;

  // Called from LookupBatch() to follow the entry chain of candidate group
  // entries filtered through group_sel. Follows the chain and uses the key
  // equality function to resolve hash collisions.
  template <bool PCIIsFiltered>
  void FollowNextLoop(ProjectedColumnsIterator *iters[], u32 num_elems, u32 group_sel[], const hash_t hashes[],
                      HashTableEntry *entries[], KeyEqFn key_eq_fn) const;

  // Called from ProcessBatch() to create missing groups
  template <bool PCIIsFiltered>
  void CreateMissingGroups(ProjectedColumnsIterator *iters[], u32 num_elems, const hash_t hashes[],
                           HashTableEntry *entries[], KeyEqFn key_eq_fn, InitAggFn init_agg_fn);

  // Called from ProcessBatch() to update only the valid entries in the input
  // vector
  template <bool PCIIsFiltered>
  void AdvanceGroups(ProjectedColumnsIterator *iters[], u32 num_elems, HashTableEntry *entries[],
                     AdvanceAggFn advance_agg_fn);

  // Called during partitioned scan to build an aggregation hash table over a
  // single partition.
  AggregationHashTable *BuildTableOverPartition(void *query_state, u32 partition_idx);

 private:
  // Memory allocator.
  MemoryPool *memory_;

  // The size of the aggregates in bytes.
  std::size_t payload_size_;

  // Where the aggregates are stored.
  util::ChunkedVector<MemoryPoolAllocator<byte>> entries_;

  // Entries taken from other tables.
  MemPoolVector<decltype(entries_)> owned_entries_;

  // The hash index.
  GenericHashTable hash_table_;

  // -------------------------------------------------------
  // Overflow partitions
  // -------------------------------------------------------

  // The function to merge a set of overflow partitions into one table.
  MergePartitionFn merge_partition_fn_;
  // The head and tail arrays over the overflow partition. These arrays are
  // allocated from the pool. Their contents are pointers into pool-allocated
  // memory, so they don't need to be deleted.
  HashTableEntry **partition_heads_;
  HashTableEntry **partition_tails_;
  // The HyperLogLog++ estimated for each overflow partition. The array is
  // allocated from the pool, but each element is new'd from libcount. Hence,
  // the elements have to be deleted.
  libcount::HLL **partition_estimates_;
  // The aggregation hash table over each partition. The array and each element
  // is allocated from the pool.
  AggregationHashTable **partition_tables_;
  // The number of elements that can be inserted into the main hash table before
  // we flush into the overflow partitions. We size this so that the entries
  // are roughly L2-sized.
  u64 flush_threshold_;
  // The number of bits to shift the hash value to determine its overflow
  // partition.
  u64 partition_shift_bits_;

  // Runtime stats.
  Stats stats_;

  // The maximum number of elements in the table before a resize.
  u64 max_fill_;
};

// ---------------------------------------------------------
// Implementation below
// ---------------------------------------------------------

inline HashTableEntry *AggregationHashTable::LookupEntryInternal(hash_t hash, AggregationHashTable::KeyEqFn key_eq_fn,
                                                                 const void *probe_tuple) const {
  HashTableEntry *entry = hash_table_.FindChainHead(hash);
  while (entry != nullptr) {
    if (entry->hash == hash && key_eq_fn(entry->payload, probe_tuple)) {
      return entry;
    }
    entry = entry->next;
  }
  return nullptr;
}

inline byte *AggregationHashTable::Lookup(hash_t hash, AggregationHashTable::KeyEqFn key_eq_fn,
                                          const void *probe_tuple) {
  auto *entry = LookupEntryInternal(hash, key_eq_fn, probe_tuple);
  return (entry == nullptr ? nullptr : entry->payload);
}

// ---------------------------------------------------------
// Aggregation Hash Table Iterator
// ---------------------------------------------------------

/**
 * An iterator over the contents of an aggregation hash table
 */
class AggregationHashTableIterator {
 public:
  /**
   * Constructor
   * @param agg_table hash table to iterator over
   */
  explicit AggregationHashTableIterator(const AggregationHashTable &agg_table) : iter_(agg_table.hash_table_) {}

  /**
   * Does this iterate have more data
   * @return True if the iterator has more data; false otherwise
   */
  bool HasNext() const { return iter_.HasNext(); }

  /**
   * Advance the iterator
   */
  void Next() { iter_.Next(); }

  /**
   * Return a pointer to the current row. It assumed the called has checked the
   * iterator is valid.
   */
  const byte *GetCurrentAggregateRow() const {
    auto *ht_entry = iter_.GetCurrentEntry();
    return ht_entry->payload;
  }

 private:
  // The iterator over the aggregation hash table
  // TODO(pmenon): Switch to vectorized iterator when perf is better
  GenericHashTableIterator<false> iter_;
};

/**
 * An iterator over a range of overflow partition entries in an aggregation hash
 * table. The range is provided through the constructor. Each overflow entry's
 * hash value is accessible through @em GetHash(), along with the opaque payload
 * through @em GetPayload().
 */
class AggregationOverflowPartitionIterator {
 public:
  /**
   * Construct an iterator over the given partition range.
   * @param partitions_begin The beginning of the range.
   * @param partitions_end The end of the range.
   */
  AggregationOverflowPartitionIterator(HashTableEntry **partitions_begin, HashTableEntry **partitions_end)
      : partitions_iter_(partitions_begin), partitions_end_(partitions_end), curr_(nullptr) {
    Next();
  }

  /**
   * Are there more overflow entries?
   * @return True if the iterator has more data; false otherwise
   */
  bool HasNext() const { return curr_ != nullptr; }

  /**
   * Move to the next overflow entry.
   */
  void Next() {
    // Try to move along current partition
    if (curr_ != nullptr) {
      curr_ = curr_->next;
      if (curr_ != nullptr) {
        return;
      }
    }

    // Find next non-empty partition
    while (curr_ == nullptr && partitions_iter_ != partitions_end_) {
      curr_ = *partitions_iter_++;
    }
  }

  /**
   * Get the hash value of the overflow entry the iterator is currently pointing
   * to. It is assumed the caller has checked there is data in the iterator.
   * @return The hash value of the current overflow entry.
   */
  const hash_t GetHash() const { return curr_->hash; }

  /**
   * Get the payload of the overflow entry the iterator is currently pointing
   * to. It is assumed the caller has checked there is data in the iterator.
   * @return The opaque payload associated with the current overflow entry.
   */
  const byte *GetPayload() const { return curr_->payload; }

  /**
   * Get the payload of the overflow entry the iterator is currently pointing
   * to, but interpret it as the given template type @em T. It is assumed the
   * caller has checked there is data in the iterator.
   * @tparam T The type of the payload in the current overflow entry.
   * @return The opaque payload associated with the current overflow entry.
   */
  template <typename T>
  const T *GetPayloadAs() const {
    return curr_->PayloadAs<T>();
  }

 private:
  // The current position in the partitions array
  HashTableEntry **partitions_iter_;
  // The ending position in the partitions array
  HashTableEntry **partitions_end_;
  // The current overflow entry
  HashTableEntry *curr_;
};

}  // namespace terrier::sql
