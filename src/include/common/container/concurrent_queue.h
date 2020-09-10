#pragma once

#include <tbb/concurrent_queue.h>

#include "common/macros.h"

namespace terrier::common {
/**
 * A thread-safe queue implementation
 * @tparam T element type
 * @tparam Alloc allocator used
 * @warning Consider the non-trivial overhead associated with a concurrent data structure before defaulting to its use.
 */
template <typename T, typename Alloc = tbb::cache_aligned_allocator<T>>
class ConcurrentQueue {
  // This wrapper is here so we are free to swap out underlying implementation
  // of the data structure or hand-craft it ourselves. Compiler should inline
  // most of it for us anyway and incur minimal overhead. (Currently using tbb
  // see https://software.intel.com/en-us/node/506200)
  //
  // Keep the interface minimalistic until we figure out what implementation to use.
 public:
  /**
   * Check emptiness
   * @return if the underlying queue has no item
   */
  bool Empty() { return queue_.empty(); }

  /**
   * Clears all elements from the queue
   */
  void Clear() { queue_.clear(); }

  /**
   * Puts the element at the tail of the queue
   * @param elem the element to enqueue
   */
  void Enqueue(T elem) { queue_.push(elem); }

  /**
   * If value is available, remove the element at the head of the queue and assign
   * it to the destination.
   * @param dest if an element exists.
   */
  bool Dequeue(T *dest) { return queue_.try_pop(*dest); }

  /**
   * Returns the number of items in the queue. The method is allowed to return
   * an approximate size if there are concurrent modifications in flight.
   * @return the approximate number of items in the queue
   */
  uint64_t UnsafeSize() const { return queue_.unsafe_size(); }

 private:
  tbb::concurrent_queue<T, Alloc> queue_;
};
}  // namespace terrier::common
