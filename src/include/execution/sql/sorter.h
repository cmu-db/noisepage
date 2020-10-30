#pragma once

#include <iterator>
#include <memory>
#include <vector>

#include "catalog/schema.h"
#include "common/macros.h"
#include "execution/sql/memory_pool.h"
#include "execution/util/chunked_vector.h"

namespace noisepage::execution::exec {
class ExecutionContext;
}

namespace noisepage::execution::sql {

class ThreadStateContainer;
class VectorProjection;
class VectorProjectionIterator;

/**
 * A Sorter collects tuple data into a buffer and sorts it. Sorters require clients to provide a
 * sorting function and the size of the entries it will store. This is because it can store and sort
 * <b>runtime-generated</b> structures, in contrast to, say, STL std::vectors that know their types
 * at compile-time.
 *
 * To insert a tuple, users invoke Sorter::AllocInputTuple() to acquire a chunk of memory where
 * the tuple's contents can be written into. This memory is guaranteed to contain sufficient memory
 * to store all of the tuples attributes if the Sorter instance was instantiated with the correct
 * tuple size.  When the insertions are complete, all tuples can be sorted through Sorter::Sort().
 *
 * @code
 * Sorter sorter(...);
 * for (...) {
 *   auto tuple = reinterpret_cast<Tuple *>(sorter.AllocInputTuple());
 *   tuple->a = ...
 *   tuple->b = ...
 *   // More attributes
 * }
 * // Now sort
 * sorter.Sort();
 * @endcode
 *
 * Sorters also support efficient Top-K. To use the Top-K functionality, users should use pairs of
 * Sorter::AllocInputTupleTopK() and Sorter::AllocInputTupleTopKFinish() before and after
 * <b>each</b> insertion, providing the size of K in each invocation. After all insertions
 * complete, the results of Sorter::Sort() will contain only Top-K elements.
 *
 * @code
 * uint32_t top_k = 20; // only interested in top 20 elements
 * Sorter sorter(...);
 * for (...) {
 *   auto tuple = reinterpret_cast<Tuple *>(sorter.AllocInputTupleTopK(top_k));
 *   tuple->a = ...
 *   tuple->b = ...
 *   // More attributes
 *   sorter.AllocInputTupleTopKFinish();
 * }
 * // Now sort
 * sorter.Sort();
 * // Sorter will only contain 20 elements
 * @endcode
 *
 * Sorters also support parallel sort and parallel Top-K. This relies on using thread-local Sorter
 * instances managed by a tpl::sql::ThreadStatesContainer. Each thread will insert into their
 * thread-local Sorter, but <b>without calling</b> Sorter::Sort(). When all insertions are complete
 * across all threads, the primary thread uses Sorter::SortParallel() or Sorter::SortTopKParallel()
 * for parallel sort and parallel Top-K, respectively.
 */
class EXPORT Sorter {
 public:
  /** Used to denote the offsets into ExecutionContext::hooks_ of particular functions */
  enum class HookOffsets : uint32_t {
    StartTLSortHook = 0,
    StartTLMergeHook,
    EndTLSortHook,
    EndTLMergeHook,
    EndSingleSorterHook,

    NUM_HOOKS
  };

  /**
   * Minimum number of tuples to have before using a parallel sort. We use a smaller value in DEBUG
   * mode to reduce runtime of tests by not requiring large Sorters when testing parallel sorts.
   */
#ifndef NDEBUG
  static constexpr uint64_t DEFAULT_MIN_TUPLES_FOR_PARALLEL_SORT = 1000;
#else
  static constexpr uint64_t DEFAULT_MIN_TUPLES_FOR_PARALLEL_SORT = 10000;
#endif

  /**
   * The comparison function used to sort tuples in a Sorter.
   */
  using ComparisonFunction = int32_t (*)(const void *lhs, const void *rhs);

  /**
   * Construct a sorter using @em memory as the memory allocator, storing tuples @em tuple_size
   * size in bytes, and using the comparison function @em cmp_fn.
   * @param exec_ctx The ExecutionContext used for executing the query
   * @param cmp_fn The sorting comparison function
   * @param tuple_size The sizes of the input tuples in bytes
   */
  Sorter(exec::ExecutionContext *exec_ctx, ComparisonFunction cmp_fn, uint32_t tuple_size);

  /**
   * Destructor.
   */
  ~Sorter();

  /**
   * This class cannot be copied or moved.
   */
  DISALLOW_COPY_AND_MOVE(Sorter);

  /**
   * Allocate room for a tuple in this sorter. It's the callers responsibility to fill in the
   * contents.
   * @return A pointer to a contiguous chunk of memory where the tuple's contents are written.
   */
  byte *AllocInputTuple();

  /**
   * Tuple allocation for TopK. This call is must be paired with a subsequent call to
   * Sorter::AllocInputTupleTopKFinish() after the tuple's contents have been written into the
   * space.
   *
   * @see AllocInputTupleTopKFinish()
   */
  byte *AllocInputTupleTopK(uint64_t top_k);

  /**
   * Complete the allocation and insertion of a tuple intended for TopK. This call must be preceded
   * by a call to Sorter::AllocInputTupleTopK().
   *
   * @see AllocInputTupleTopK()
   */
  void AllocInputTupleTopKFinish(uint64_t top_k);

  /**
   * Sort all inserted entries.
   */
  void Sort();

  /**
   * Perform a parallel sort of all sorter instances stored in the thread state container object.
   * Each thread-local sorter instance is assumed (but not required) to be unsorted. Once sorting
   * completes, <b>this</b> sorter instance will take ownership of all data owned by each
   * thread-local instances.
   * @param thread_state_container The container holding all thread-local sorter instances.
   * @param sorter_offset The offset into the container where the sorter instance is.
   */
  void SortParallel(ThreadStateContainer *thread_state_container, std::size_t sorter_offset);

  /**
   * Perform a parallel Top-K of all sorter instances stored in the thread state container object.
   * Each thread-local sorter instance is assumed (but not required) to be unsorted. Once sorting
   * completes, this sorter instance will take ownership of all data owned by each thread-local
   * instances.
   * @param thread_state_container The container holding all thread-local sorter instances.
   * @param sorter_offset The offset into the container where the sorter instance is.
   * @param top_k The number entries at the top the caller cares for.
   */
  void SortTopKParallel(ThreadStateContainer *thread_state_container, uint32_t sorter_offset, uint64_t top_k);

  /**
   * @return The number of tuples currently in this sorter.
   */
  uint64_t GetTupleCount() const noexcept { return tuples_.size(); }

  /**
   * @return True if this sorter contains no tuples; false otherwise.
   */
  bool IsEmpty() const noexcept { return GetTupleCount() == 0; }

  /**
   * @return True if this sorter's contents been sorted; false otherwise.
   */
  bool IsSorted() const noexcept { return sorted_; }

 private:
  // Build a max heap from the tuples currently stored in the sorter instance
  void BuildHeap();

  // Sift down the element at the root of the heap while maintaining the heap
  // property
  void HeapSiftDown();

 private:
  friend class SorterIterator;
  friend class SorterVectorIterator;

  exec::ExecutionContext *exec_ctx_;
  // Memory pool
  MemoryPool *memory_;

  // The vector that stores tuple data
  util::ChunkedVector<MemoryPoolAllocator<byte>> tuple_storage_;

  // All tuples this sorter has taken ownership of from thread-local sorters, if any
  MemPoolVector<decltype(tuple_storage_)> owned_tuples_;

  // The function used to compare two tuples
  ComparisonFunction cmp_fn_;

  // Vector of pointers to each entry. This is the vector that's sorted.
  MemPoolVector<const byte *> tuples_;

  // Flag indicating if the contents of the sorter have been sorted
  bool sorted_;
};

/**
 * An iterator over the elements in a sorter instance.
 */
class SorterIterator {
  using IteratorType = decltype(Sorter::tuples_)::const_iterator;

 public:
  /**
   * Create an iterator over the provided sorter.
   * @param sorter The sorter instance.
   */
  explicit SorterIterator(const Sorter &sorter);

  /**
   * @return True if the iterator has more data; false otherwise.
   */
  bool HasNext() const { return iter_ != end_; }

  /**
   * Advance the iterator by one tuple.
   */
  void Next() { ++iter_; }

  /**
   * Advance the iterator by @em n rows. If there are fewer than @em n rows remaining in this
   * iterator, it will self-exhaust itself and subsequent calls to HasNext() will return false.
   * @param n The number of rows to advance by.
   */
  void AdvanceBy(uint64_t n);

  /**
   * @return The number of tuples remaining in the iterator.
   */
  uint64_t NumRemaining() const { return std::distance(iter_, end_); }

  /**
   * @return A pointer to the current row. It assumed the called has checked the iterator is valid.
   */
  const byte *GetRow() const {
    NOISEPAGE_ASSERT(iter_ != end_, "Invalid iterator");
    return *iter_;
  }

  /**
   * @return A pointer to the current row interpreted as the template type @em T. It assumed the
   *         called has checked the iterator is valid.
   */
  template <typename T>
  const T *GetRowAs() const {
    return reinterpret_cast<const T *>(GetRow());
  }

  // -------------------------------------------------------
  // Operator overloads
  // -------------------------------------------------------

  /**
   * @return A pointer to the current iteration row.
   */
  const byte *operator*() const { return GetRow(); }

  /**
   * @return A reference to this iterator after it's been advanced one row.
   */
  SorterIterator &operator++() {
    Next();
    return *this;
  }

 private:
  // The current iterator position
  IteratorType iter_;
  // The ending iterator position
  const IteratorType end_;
};

/**
 * A vectorized iterator over the elements in a sorter instance.
 */
class SorterVectorIterator {
 public:
  /** Function that converts row-oriented aggregate data to a column-oriented vector projection. */
  using TransposeFn = void (*)(const byte **, uint64_t, VectorProjectionIterator *);

  /**
   * Construct a vector iterator over the given sorter instance.
   */
  SorterVectorIterator(const Sorter &sorter, const std::vector<const catalog::Schema::Column *> &column_info,
                       TransposeFn transpose_fn);

  /**
   * Construct a vector iterator over the given sorter instance.
   */
  SorterVectorIterator(const Sorter &sorter, const catalog::Schema::Column *column_info, uint32_t num_cols,
                       TransposeFn transpose_fn);

  /**
   * Destructor.
   */
  ~SorterVectorIterator();

  /**
   * Does this iterator have more data?
   */
  bool HasNext() const;

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
  // The memory pool
  MemoryPool *memory_;

  // The current and ending iterator positions, respectively.
  SorterIterator iter_;

  // Temporary array storing the sorter rows
  const byte **temp_rows_;

  // The vector projections produced by this iterator
  std::unique_ptr<VectorProjection> vector_projection_;

  // The iterator over the vector projection
  std::unique_ptr<VectorProjectionIterator> vector_projection_iterator_;
};

}  // namespace noisepage::execution::sql
