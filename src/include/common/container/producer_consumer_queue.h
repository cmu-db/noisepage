#pragma once

#include <utility>
#include "common/macros.h"
#include "spdlog/details/mpmc_blocking_q.h"

namespace terrier::common {
/**
 * A thread-safe producer consumer queue implementation
 * @tparam T element type
 * @warning Consider the non-trivial overhead associated with a concurrent data structure before defaulting to its use.
 */
template <typename T>
class ProducerConsumerQueue {
  // This wrapper is here so we are free to swap out underlying implementation
  // of the data structure or hand-craft it ourselves. Compiler should inline
  // most of it for us anyway and incur minimal overhead. (Currently using spdlog
  // see https://github.com/gabime/spdlog)
  //
  // Keep the interface minimalistic until we figure out what implementation to use.
 public:
  /**
   *
   * @param size Size of the queue
   * @param wait_time Timeout for non-blocking enqueue/dequeue operations
   */
  ProducerConsumerQueue(size_t size, std::chrono::milliseconds wait_time) : queue_(size), wait_time_(wait_time) {}

  /**
   * Blocks until there is room in the queue and then puts the element at the tail of the queue
   * @param elem the element to enqueue
   */
  void BlockingEnqueue(T elem) { queue_.enqueue(std::move(elem)); }

  /**
   * Puts the element at the tail of the queue but does not block
   * Overwrites oldest element in the queue if no room left
   * @param elem the element to enqueue
   */
  void NonBlockingEnqueue(T elem) { queue_.enqueue_nowait(std::move(elem)); }

  /**
   * Blocks until a value is available, then remove the element at the head of the queue and assign
   * it to the destination.
   * @param dest pointer to the object which will hold the dequeued element
   */
  void BlockingDequeue(T *dest) {
    bool dequeued = false;
    while (!dequeued) {
      // Get a buffer from the queue of buffers pending write
      dequeued = queue_.dequeue_for(*dest, wait_time_);
    }
  }

  /**
   * If value is available, remove the element at the head of the queue and assign
   * it to the destination.
   * @param dest pointer to the object which may hold the dequeued element
   * @return true if dequeue was successful, false otherwise
   */
  bool NonBlockingDequeue(T *dest) { return queue_.dequeue_for(*dest, wait_time_); }

 private:
  spdlog::details::mpmc_blocking_queue<T> queue_;
  std::chrono::milliseconds wait_time_;
};
}  // namespace terrier::common
