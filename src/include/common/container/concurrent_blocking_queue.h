#pragma once

#include <tbb/concurrent_queue.h>

#include "common/macros.h"

namespace noisepage::common {
/**
 * A thread-safe blocking queue implementation
 * @tparam T element type
 * @tparam Alloc allocator used
 * @warning Consider the non-trivial overhead associated with a concurrent data structure before defaulting to its use.
 */
template <typename T, typename Alloc = tbb::cache_aligned_allocator<T>>
class ConcurrentBlockingQueue {
  // This wrapper is here so we are free to swap out underlying implementation
  // of the data structure or hand-craft it ourselves. Compiler should inline
  // most of it for us anyway and incur minimal overhead. (Currently using tbb
  // see https://software.intel.com/en-us/node/506201)
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
   * Block until a value is available, remove the element at the head of the
   * queue and assign it to the destination.
   * @param dest an element.
   */
  void Dequeue(T *dest) { queue_.pop(*dest); }

  /**
   * Returns the number of items in the queue. The method is allowed to return
   * an approximate size if there are concurrent modifications in flight.
   * @return the approximate number of items in the queue
   */
  uint64_t UnsafeSize() const { return queue_.size(); }

 private:
  tbb::concurrent_bounded_queue<T, Alloc> queue_;
};
}  // namespace noisepage::common
