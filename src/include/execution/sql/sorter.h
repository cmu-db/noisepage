#pragma once

#include <vector>

#include "execution/sql/memory_pool.h"
#include "execution/util/chunked_vector.h"
#include "execution/util/macros.h"

namespace terrier::execution::sql {

class ThreadStateContainer;

/**
 * Sorters
 */
class Sorter {
 public:
  /**
   * The interface of the comparison function used to sort tuples
   */
  using ComparisonFunction = i32 (*)(const void *lhs, const void *rhs);

  /**
   * Construct a sorter using @em memory as the memory allocator, storing tuples
   * @em tuple_size size in bytes, and using the comparison function @em cmp_fn.
   * @param memory The memory pool to allocate memory from
   * @param cmp_fn The sorting comparison function
   * @param tuple_size The sizes of the input tuples in bytes
   */
  Sorter(MemoryPool *memory, ComparisonFunction cmp_fn, u32 tuple_size);

  /**
   * Destructor
   */
  ~Sorter();

  /**
   * This class cannot be copied or moved
   */
  DISALLOW_COPY_AND_MOVE(Sorter);

  /**
   * Allocate space for an entry in this sorter, returning a pointer with
   * at least \a tuple_size contiguous bytes
   */
  byte *AllocInputTuple();

  /**
   * Tuple allocation for TopK. This call is must be paired with a subsequent
   * @em AllocInputTupleTopKFinish() call.
   *
   * @see AllocInputTupleTopKFinish()
   */
  byte *AllocInputTupleTopK(u64 top_k);

  /**
   * Complete the allocation and insertion of a tuple intended for TopK. This
   * call must be preceded by a call to @em AllocInputTupleTopK().
   *
   * @see AllocInputTupleTopK()
   */
  void AllocInputTupleTopKFinish(u64 top_k);

  /**
   * Sort all inserted entries
   */
  void Sort();

  /**
   * Perform a parallel sort of all sorter instances stored in the thread state
   * container object. Each thread-local sorter instance is assumed (but not
   * required) to be unsorted. Once sorting completes, this sorter instance will
   * take ownership of all data owned by each thread-local instances.
   * @param thread_state_container The container holding all thread-local sorter
   *                               instances.
   * @param sorter_offset The offset into the container where the sorter
   *                      instance is.
   */
  void SortParallel(const ThreadStateContainer *thread_state_container, u32 sorter_offset);

  /**
   * Perform a parallel Top-K of all sorter instances stored in the thread
   * state container object. Each thread-local sorter instance is assumed (but
   * not required) to be unsorted. Once sorting completes, this sorter instance
   * will take ownership of all data owned by each thread-local instances.
   * @param thread_state_container The container holding all thread-local sorter
   *                               instances.
   * @param sorter_offset The offset into the container where the sorter
   *                      instance is.
   * @param top_k The number entries at the top the caller cares for.
   */
  void SortTopKParallel(const ThreadStateContainer *thread_state_container, u32 sorter_offset, u64 top_k);

  /**
   * Return the number of tuples currently in this sorter
   */
  u64 NumTuples() const { return tuples_.size(); }

  /**
   * Has this sorter's contents been sorted?
   */
  bool is_sorted() const { return sorted_; }

 private:
  // Build a max heap from the tuples currently stored in the sorter instance
  void BuildHeap();

  // Sift down the element at the root of the heap while maintaining the heap
  // property
  void HeapSiftDown();

 private:
  friend class SorterIterator;

  // Vector of entries
  util::ChunkedVector<MemoryPoolAllocator<byte>> tuple_storage_;

  // All tuples this sorter has taken ownership of from thread-local sorters
  MemPoolVector<util::ChunkedVector<MemoryPoolAllocator<byte>>> owned_tuples_;

  // The comparison function
  ComparisonFunction cmp_fn_;

  // Vector of pointers to each entry. This is the vector that's sorted.
  MemPoolVector<const byte *> tuples_;

  // Flag indicating if the contents of the sorter have been sorted
  bool sorted_;
};

/**
 * An iterator over the elements in a sorter instance
 */
class SorterIterator {
  /**
   * Type of the iterator
   */
  using IteratorType = decltype(Sorter::tuples_)::iterator;

 public:
  /**
   * Constructor
   * @param sorter sorter to iterate over
   */
  explicit SorterIterator(Sorter *sorter) noexcept : iter_(sorter->tuples_.begin()), end_(sorter->tuples_.end()) {}

  /**
   * Dereference operator
   * @return A pointer to the current iteration row
   */
  const byte *operator*() const noexcept { return *iter_; }

  /**
   * Pre-increment the iterator
   * @return A reference to this iterator after it's been advanced one row
   */
  SorterIterator &operator++() noexcept {
    ++iter_;
    return *this;
  }

  /**
   * Does this iterate have more data
   * @return True if the iterator has more data; false otherwise
   */
  bool HasNext() const { return iter_ != end_; }

  /**
   * Advance the iterator
   */
  void Next() { this->operator++(); }

  /**
   * Return a pointer to the current row. It assumed the called has checked the
   * iterator is valid.
   */
  const byte *GetRow() const {
    TPL_ASSERT(iter_ != end_, "Invalid iterator");
    return this->operator*();
  }

  /**
   * Return a pointer to the current row, interpreted as the template type
   * @em T. It assumed the called has checked the iterator is valid.
   */
  template <typename T>
  const T *GetRowAs() const {
    return reinterpret_cast<const T *>(GetRow());
  }

 private:
  // The current iterator position
  IteratorType iter_;
  // The ending iterator position
  const IteratorType end_;
};

}  // namespace terrier::execution::sql
