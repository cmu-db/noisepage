#pragma once

#include <functional>
#include <memory>
#include <utility>
#include <vector>

#include "catalog/schema.h"
#include "common/managed_pointer.h"
#include "execution/sql/chaining_hash_table.h"
#include "execution/sql/memory_pool.h"
#include "execution/sql/vector.h"
#include "execution/sql/vector_projection.h"
#include "execution/util/chunked_vector.h"
#include "execution/util/execution_common.h"

namespace libcount {
class HLL;
}  // namespace libcount

namespace noisepage::execution::exec {
class ExecutionSettings;
class ExecutionContext;
}  // namespace noisepage::execution::exec

namespace noisepage::execution::sql {

class ThreadStateContainer;
class VectorProjectionIterator;

// Forward declare
class AHTIterator;
class AHTVectorIterator;
class AHTOverflowPartitionIterator;

//===----------------------------------------------------------------------===//
//
// Aggregation Hash Table
//
//===----------------------------------------------------------------------===//

/**
 * The hash table used when performing aggregations.
 */
class EXPORT AggregationHashTable {
 public:
  /** Used to denote the offsets into ExecutionContext::hooks_ of particular functions */
  enum class HookOffsets : uint32_t {
    StartHook = 0,
    EndHook,

    NUM_HOOKS
  };

  /** The default load factor we allow the hash table to reach before resizing. */
  static constexpr const float DEFAULT_LOAD_FACTOR = 0.7f;

  /** The default initial size we set the hash table on construction. */
  static constexpr const uint32_t DEFAULT_INITIAL_TABLE_SIZE = 256;

  /** The default number of partitions we use in partitioned aggregation mode. */
  static constexpr uint32_t DEFAULT_NUM_PARTITIONS = 512;

  /** The default precision used to configure the HyperLogLog instances. Set to optimize accuracy and space manually. */
  static constexpr uint32_t DEFAULT_HLL_PRECISION = 10;

  // -------------------------------------------------------
  // Callback functions to customize aggregations
  // -------------------------------------------------------

  /**
   * Function to check the key equality of an input tuple and an existing entry in the hash table.
   * Convention: First argument is the aggregate entry, second argument is the input tuple.
   */
  using KeyEqFn = bool (*)(const void *, const void *);

  /**
   * Function to initialize a new aggregate.
   * Convention: First argument is the aggregate to initialize, second argument is the input tuple
   *             to initialize the aggregate with.
   */
  using VectorInitAggFn = void (*)(VectorProjectionIterator *, VectorProjectionIterator *);

  /**
   * Function to advance an existing aggregate with a new input value.
   * Convention: First argument is the existing aggregate to update, second argument is the input
   *             tuple to update the aggregate with.
   */
  using VectorAdvanceAggFn = void (*)(VectorProjectionIterator *, VectorProjectionIterator *);

  /**
   * Function to merge a set of overflow partitions into the given aggregation hash table.
   * Convention: First argument is an opaque state object that the user provides. The second
   *             argument is the aggregation to be built. The third argument is the list of overflow
   *             partitions pointers, and the fourth and fifth argument are the range of overflow
   *             partitions to merge into the input aggregation hash table.
   */
  using MergePartitionFn = void (*)(void *, AggregationHashTable *, AHTOverflowPartitionIterator *);

  /**
   * Function to scan an aggregation hash table.
   * Convention: First argument is query state, second argument is thread-local state, last argument
   *             is the aggregation hash table to scan.
   */
  using ScanPartitionFn = void (*)(void *, void *, const AggregationHashTable *);

  /**
   * Small class to capture various usage stats
   */
  struct Stats {
    /** Number of times that the hash table has grown. */
    uint64_t num_growths_ = 0;
    /** Number of times that the hash table has been flushed. */
    uint64_t num_flushes_ = 0;
    /** Number of times that the hash table has been inserted into. */
    uint64_t num_inserts_ = 0;
  };

  // -------------------------------------------------------
  // Main API
  // -------------------------------------------------------

  /**
   * Construct an aggregation hash table using the provided memory pool, and configured to store
   * aggregates of size @em payload_size in bytes.
   * @param exec_settings The execution settings to run with.
   * @param exec_ctx The execution context being used to run the query.
   * @param payload_size The size of the elements in the hash table, in bytes.
   */
  AggregationHashTable(const exec::ExecutionSettings &exec_settings, exec::ExecutionContext *exec_ctx,
                       std::size_t payload_size);

  /**
   * Construct an aggregation hash table using the provided memory pool, configured to store
   * aggregates of size @em payload_size in bytes, and whose initial size allows for
   * @em initial_size aggregates.
   * @param exec_settings The execution settings to run with.
   * @param exec_ctx The execution context being used to run the query.
   * @param payload_size The size of the elements in the hash table, in bytes.
   * @param initial_size The initial number of aggregates to support.
   */
  AggregationHashTable(const exec::ExecutionSettings &exec_settings, exec::ExecutionContext *exec_ctx,
                       std::size_t payload_size, uint32_t initial_size);

  /**
   * This class cannot be copied or moved.
   */
  DISALLOW_COPY_AND_MOVE(AggregationHashTable);

  /**
   * Destructor.
   */
  ~AggregationHashTable();

  /**
   * Insert a new element with hash value @em hash into the aggregation table.
   * @param hash The hash value of the element to insert.
   * @return A pointer to a memory area where the element can be written to.
   */
  byte *AllocInputTuple(hash_t hash);

  /**
   * Insert a new element with hash value @em hash into this partitioned aggregation hash table.
   * @param hash The hash value of the element to insert.
   * @return A pointer to a memory area where the input element can be written.
   */
  byte *AllocInputTuplePartitioned(hash_t hash);

  /**
   * Insert and link in an entry containing a fully-constructed tuple into this aggregation table.
   * The entry is inserted assuming non-concurrent insertions!
   * @param entry The entry to insert into the hash table.
   */
  void Insert(HashTableEntry *entry) { hash_table_.Insert<false>(entry); }

  /**
   * Lookup and return an entry in the aggregation table that matches a given hash and key. The hash
   * value is provided here, keys are checked using the provided callback function.
   * @param hash The hash value to use for early filtering.
   * @param key_eq_fn The key-equality function to resolve hash collisions.
   * @param probe_tuple The probe tuple.
   * @return A pointer to the matching entry payload; null if no entry is found.
   */
  byte *Lookup(hash_t hash, KeyEqFn key_eq_fn, const void *probe_tuple);

  /**
   * Ingest and process a batch of input into the aggregation table.
   * @param input_batch The vector projection to process.
   * @param key_indexes The ordered list of key indexes in the input batch.
   * @param init_agg_fn Function to initialize a new aggregate.
   * @param advance_agg_fn Function to advance an existing aggregate.
   * @param partitioned_aggregation Whether to perform insertions in partitioned mode.
   */
  void ProcessBatch(VectorProjectionIterator *input_batch, const std::vector<uint32_t> &key_indexes,
                    VectorInitAggFn init_agg_fn, VectorAdvanceAggFn advance_agg_fn, bool partitioned_aggregation);

  /**
   * Transfer all data and entries in each thread-local aggregation hash table (in the thread state
   * container) into this hash table.
   *
   * This function only moves memory around, no aggregation hash tables are built. It is used at the
   * end of the build-portion of a parallel aggregation before the thread state container is reset
   * for the next pipeline's thread-local state.
   *
   * @param thread_states Container for all thread-local tables.
   * @param agg_ht_offset The offset in the container to find the table.
   * @param merge_partition_fn The function to use for merging partitions.
   */
  void TransferMemoryAndPartitions(ThreadStateContainer *thread_states, std::size_t agg_ht_offset,
                                   MergePartitionFn merge_partition_fn);

  /**
   * Execute a serial scan over this hash table. It is assumed that the hash table was constructed
   * in a partitioned manner, otherwise use a simple tpl::sql::AHTITerator. This function builds a
   * hash table for any non-empty  overflow partition (if one doesn't exist), merges the contents of
   * the partition (using the merging function provided to the call to
   * @em TransferMemoryAndPartitions()), and invokes the scan callback function.
   *
   * @param query_state The (opaque) query state.
   * @param scan_fn The callback scan function, called once for each overflow partition hash table.
   */
  void ExecutePartitionedScan(void *query_state, ScanPartitionFn scan_fn);

  /**
   * Execute a parallel scan over this hash table. It is assumed that this aggregation table was
   * constructed in a partitioned manner. This function builds a hash table for any non-empty
   * overflow partition (if one doesn't exist), merges the contents of the partition (using the
   * merging function provided to the call to @em TransferMemoryAndPartitions()), and invokes the
   * scan callback function. All steps are performed in parallel; hence, the callback function
   * must be thread-safe.
   *
   * The thread states container is assumed to already have been configured prior to this scan call.
   *
   * The callback scan function accepts three arguments of which the first two are opaque state
   * objects: a query state and a thread state. The query state is provided by the user here. The
   * thread state is pulled from the provided ThreadStateContainer object for the thread.
   *
   * @param query_state The (opaque) query state.
   * @param thread_states The container holding all thread states.
   * @param scan_fn The callback scan function, called once for each overflow partition hash table.
   */
  void ExecuteParallelPartitionedScan(void *query_state, ThreadStateContainer *thread_states, ScanPartitionFn scan_fn);

  /**
   * Construct a new aggregation hash table instance for each non-empty overflow partition.
   * @param query_state An opaque state object pointer
   */
  void BuildAllPartitions(void *query_state);

  /**
   * Repartition all data stored in this partitioned hash table.
   */
  void Repartition();

  /**
   * Merge data stored in this aggregation hash table's overflow partitions into the provided
   * target aggregation hash table. Both source and target aggregation hash tables must already be
   * partitioned and must use the same partitioning key!
   * @param target The target hash table we merge our overflow partitions into.
   * @param query_state An opaque state object pointer.
   * @param merge_func The function we use to merge entries from our hash table into the target.
   */
  void MergePartitions(AggregationHashTable *target, void *query_state, MergePartitionFn merge_func);

  /**
   * @return The total number of tuples in this table.
   */
  uint64_t GetTupleCount() const { return hash_table_.GetElementCount(); }

  /**
   * @return Number of insertions into the aggregation hash table
   *
   * @note this function differs from GetTupleCount() in that it is not affected
   * by the behavior of overflow partitions.
   */
  uint64_t GetInsertCount() const { return stats_.num_inserts_; }

  /**
   * @return A read-only view of this aggregation table's statistics.
   */
  const Stats *GetStatistics() const { return &stats_; }

  // Specialized hash table mapping hash values to group IDs
  class HashToGroupIdMap;

 private:
  friend class AHTIterator;
  friend class AHTVectorIterator;

  // Does the hash table need to grow?
  bool NeedsToGrow() const noexcept { return hash_table_.GetElementCount() >= max_fill_; }

  // Grow the hash table
  void Grow();

  // Internal entry allocation + hash table linkage. Does not resize!
  HashTableEntry *AllocateEntryInternal(hash_t hash);

  // Lookup a hash table entry internally
  HashTableEntry *LookupEntryInternal(hash_t hash, KeyEqFn key_eq_fn, const void *probe_tuple) const;

  // Should we flush entries from the main table into the overflow partitions?
  bool NeedsToFlushToOverflowPartitions() const noexcept { return hash_table_.GetElementCount() >= flush_threshold_; }

  // Flush all entries currently stored in the main hash table into the overflow
  // partitions.
  void FlushToOverflowPartitions();

  // Allocate all overflow partition information if unallocated
  void AllocateOverflowPartitions();

  // Called from ProcessBatch() to compute hash values for tuples in batch.
  void ComputeHash(VectorProjectionIterator *input_batch, const std::vector<uint32_t> &key_indexes);

  // Called from ProcessBatch() to find candidate groups for tuples in batch.
  void FindGroups(VectorProjectionIterator *input_batch, const std::vector<uint32_t> &key_indexes);

  // Called from FindGroups() to lookup initial candidate aggregate entries for
  // tuples in batch.
  void LookupInitial();

  // Called from FindGroups() to check the equality of keys.
  void CheckKeyEquality(VectorProjectionIterator *input_batch, const std::vector<uint32_t> &key_indexes);

  // Called from FindGroups() to follow the entry chain of candidate groups.
  void FollowNext();

  // Called from ProcessBatch() to create and initialize new aggregates for
  // tuples that did not find a matching group.
  void CreateMissingGroups(VectorProjectionIterator *input_batch, const std::vector<uint32_t> &key_indexes,
                           VectorInitAggFn init_agg_fn);

  // Called from ProcessBatch() to update aggregates with tuples from batch that
  // found matching group.
  void AdvanceGroups(VectorProjectionIterator *input_batch, VectorAdvanceAggFn advance_agg_fn);

  // Called during partitioned (parallel) scan to build an aggregation hash
  // table over a single partition.
  AggregationHashTable *GetOrBuildTableOverPartition(void *query_state, uint32_t partition_idx);

 private:
  // A helper class containing various data structures used during batch processing.
  class BatchProcessState {
   public:
    // Constructor
    explicit BatchProcessState(std::unique_ptr<libcount::HLL> estimator,
                               std::unique_ptr<HashToGroupIdMap> hash_to_group_map);

    // Destructor
    ~BatchProcessState();

    // Reset state in preparation for processing the next batch.
    void Reset(VectorProjectionIterator *input_batch);

    libcount::HLL *HLL() { return hll_estimator_.get(); }
    VectorProjection *Projection() { return &hash_and_entries_; }
    Vector *Hashes() { return hash_and_entries_.GetColumn(0); }
    Vector *Entries() { return hash_and_entries_.GetColumn(1); }
    TupleIdList *GroupsFound() { return &groups_found_; }
    TupleIdList *GroupsNotFound() { return &groups_not_found_; }
    TupleIdList *KeyNotEqual() { return &key_not_equal_; }
    TupleIdList *KeyEqual() { return &key_equal_; }
    HashToGroupIdMap *HashToGroupMap() { return hash_to_group_map_.get(); }

   private:
    // Unique hash estimator
    std::unique_ptr<libcount::HLL> hll_estimator_;
    // Specialized structure mapping hashes to group IDs
    std::unique_ptr<HashToGroupIdMap> hash_to_group_map_;
    // Projection containing hashes and entries
    VectorProjection hash_and_entries_;
    // List of tuples that do not have a matching group
    TupleIdList groups_not_found_;
    // List of tuples that have found a matching group
    TupleIdList groups_found_;
    // The list of groups that have unmatched keys
    TupleIdList key_not_equal_;
    TupleIdList key_equal_;
  };

 private:
  // Execution context
  const exec::ExecutionSettings &exec_settings_;

  exec::ExecutionContext *exec_ctx_;

  // Memory allocator.
  MemoryPool *memory_;

  // The size of the aggregates in bytes.
  std::size_t payload_size_;

  // Where the aggregates are stored.
  util::ChunkedVector<MemoryPoolAllocator<byte>> entries_;

  // Entries taken from other tables.
  MemPoolVector<decltype(entries_)> owned_entries_;

  // The hash index.
  UntaggedChainingHashTable hash_table_;

  // State used during batch processing.
  MemPoolPtr<BatchProcessState> batch_state_;

  // -------------------------------------------------------
  // Overflow partitions
  // -------------------------------------------------------

  // The function to merge a set of overflow partitions into one table.
  MergePartitionFn merge_partition_fn_;
  // The head and tail arrays over the overflow partition. These arrays and each
  // element in them are allocated from the pool, so they don't need to be
  // explicitly deleted.
  HashTableEntry **partition_heads_;
  HashTableEntry **partition_tails_;
  // The HyperLogLog++ estimated for each overflow partition. The array is
  // allocated from the pool, but each element is allocated from libcount. Thus,
  // we need to delete manually before freeing the array.
  libcount::HLL **partition_estimates_;
  // The aggregation hash table over each partition. The array and each element
  // is allocated from the pool, so they don't need to be explicitly deleted.
  AggregationHashTable **partition_tables_;
  // The number of elements that can be inserted into the main hash table before
  // we flush into the overflow partitions. We size this so that the entries are
  // roughly L2-sized.
  uint64_t flush_threshold_;
  // The number of bits to shift the hash value to determine the overflow
  // partition an entry is linked into.
  uint64_t partition_shift_bits_;

  // Runtime stats.
  Stats stats_;

  // The maximum number of elements in the table before a resize.
  uint64_t max_fill_;
};

// ---------------------------------------------------------
// Aggregation Hash Table implementation below
// ---------------------------------------------------------

inline HashTableEntry *AggregationHashTable::LookupEntryInternal(hash_t hash, AggregationHashTable::KeyEqFn key_eq_fn,
                                                                 const void *probe_tuple) const {
  HashTableEntry *entry = hash_table_.FindChainHead(hash);
  while (entry != nullptr) {
    if (entry->hash_ == hash && key_eq_fn(entry->payload_, probe_tuple)) {
      return entry;
    }
    entry = entry->next_;
  }
  return nullptr;
}

inline byte *AggregationHashTable::Lookup(hash_t hash, AggregationHashTable::KeyEqFn key_eq_fn,
                                          const void *probe_tuple) {
  auto *entry = LookupEntryInternal(hash, key_eq_fn, probe_tuple);
  return (entry == nullptr ? nullptr : entry->payload_);
}

//===----------------------------------------------------------------------===//
//
// Aggregation Hash Table Iterator
//
//===----------------------------------------------------------------------===//

/**
 * A tuple-at-a-time iterator over the contents of an aggregation hash table.
 */
class AHTIterator {
 public:
  /**
   * Construct an iterator over the given aggregation hash table.
   * @param agg_table The table to iterate.
   */
  explicit AHTIterator(const AggregationHashTable &agg_table) : iter_(agg_table.hash_table_) {}

  /**
   * @return True if the iterator has more data; false otherwise
   */
  bool HasNext() const { return iter_.HasNext(); }

  /**
   * Advance the iterator one tuple.
   */
  void Next() { iter_.Next(); }

  /**
   * @return A pointer to the current row. This assumes a previous call to HasNext() indicated there
   *         is more data.
   */
  const byte *GetCurrentAggregateRow() const {
    auto *ht_entry = iter_.GetCurrentEntry();
    return ht_entry->payload_;
  }

 private:
  // The iterator over the aggregation hash table
  ChainingHashTableIterator<false> iter_;
};

//===----------------------------------------------------------------------===//
//
// Aggregation Hash Table Vector Iterator
//
//===----------------------------------------------------------------------===//

/**
 * A vectorized iterator over the contents of an aggregation hash table. This exists so that users
 * can generate vector projections from aggregation hash tables, which might be useful when feeding
 * into other operations or performing filters post aggregation.
 *
 * To facilitate this, users must provide a function that converts row-oriented aggregate data into
 * column-oriented vector projections, i.e., a transpose function.
 */
class AHTVectorIterator {
 public:
  /** Function that converts row-oriented aggregate data to a column-oriented vector projection. */
  using TransposeFn = void (*)(const HashTableEntry *[], uint64_t, VectorProjectionIterator *);

  /**
   * Construct a vector iterator over the given aggregation table.
   */
  AHTVectorIterator(const AggregationHashTable &agg_hash_table,
                    const std::vector<const catalog::Schema::Column *> &column_info, TransposeFn transpose_fn);

  /**
   * Construct a vector iterator over the given aggregation table.
   */
  AHTVectorIterator(const AggregationHashTable &agg_hash_table, const catalog::Schema::Column *column_info,
                    uint32_t num_cols, TransposeFn transpose_fn);

  /**
   * This class cannot be copied or moved.
   */
  DISALLOW_COPY_AND_MOVE(AHTVectorIterator);

  /**
   * Does this iterator have more data?
   */
  bool HasNext() const { return iter_.HasNext(); }

  /**
   * Advance the iterator by, at most, one vector's worth of data.
   */
  void Next(TransposeFn transpose_fn);

  /**
   * Return the next vector output.
   */
  VectorProjectionIterator *GetVectorProjectionIterator() { return vector_projection_iterator_.get(); }

 private:
  void BuildVectorProjection(TransposeFn transpose_fn);

 private:
  // Memory allocator
  MemoryPool *memory_;

  // The vectorized iterate over the hash table
  ChainingHashTableVectorIterator<false> iter_;

  // The vector projection containing aggregate data stored column-wise
  std::unique_ptr<VectorProjection> vector_projection_;

  // An iterator over the produced vector projection
  std::unique_ptr<VectorProjectionIterator> vector_projection_iterator_;
};

//===----------------------------------------------------------------------===//
//
// Aggregation Hash Table Overflow Partitions Iterator
//
//===----------------------------------------------------------------------===//

/**
 * An iterator over a range of overflow partition entries in an aggregation hash table. The range is
 * provided through the constructor. Each overflow entry's hash value is accessible through
 * @em GetHash(), along with the opaque payload through @em GetPayload().
 */
class AHTOverflowPartitionIterator {
 public:
  /**
   * Construct an iterator over the given partition range.
   * @param partitions_begin The beginning of the range.
   * @param partitions_end The end of the range.
   */
  AHTOverflowPartitionIterator(HashTableEntry **partitions_begin, HashTableEntry **partitions_end)
      : partitions_iter_(partitions_begin), partitions_end_(partitions_end), curr_(nullptr), next_(nullptr) {
    // First find a partition that has a chain
    FindPartitionHead();
    if (next_ != nullptr) {
      Next();
    }
  }

  /**
   * @return True if the iterator has more data; false otherwise.
   */
  bool HasNext() const { return curr_ != nullptr; }

  /**
   * Move to the next overflow entry.
   */
  void Next() {
    curr_ = next_;

    if (LIKELY(next_ != nullptr)) {
      next_ = next_->next_;

      // If 'next_' is NULL, we'are the end of a partition. Find the next non-empty partition.
      if (next_ == nullptr) {
        FindPartitionHead();
      }
    }
  }

  /**
   * @return The current entry container for the current row.
   */
  HashTableEntry *GetEntryForRow() const { return curr_; }

  /**
   * @return The hash value of the current row.
   */
  hash_t GetRowHash() const {
    NOISEPAGE_ASSERT(curr_ != nullptr, "Iterator not pointing to an overflow entry");
    return curr_->hash_;
  }

  /**
   * @return The contents of the current row.
   */
  const byte *GetRow() const {
    NOISEPAGE_ASSERT(curr_ != nullptr, "Iterator not pointing to an overflow entry");
    return curr_->payload_;
  }

  /**
   * @tparam The type of the contents of the current row.
   * @return The contents of the current row as type @em T.
   */
  template <typename T>
  const T *GetRowAs() const {
    NOISEPAGE_ASSERT(curr_ != nullptr, "Iterator not pointing to an overflow entry");
    return curr_->PayloadAs<T>();
  }

 private:
  void FindPartitionHead() {
    while (next_ == nullptr && partitions_iter_ != partitions_end_) {
      next_ = *partitions_iter_++;
    }
  }

 private:
  // The current position in the partitions array
  HashTableEntry **partitions_iter_;

  // The ending position in the partitions array
  HashTableEntry **partitions_end_;

  // The current overflow entry
  HashTableEntry *curr_;

  // The next value overflow entry
  HashTableEntry *next_;
};

}  // namespace noisepage::execution::sql
