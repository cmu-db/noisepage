#pragma once

#include <tbb/enumerable_thread_specific.h>

namespace terrier::execution::sql {

/**
 * TODO: track memory usage
 */
class MemoryTracker {
 public:
  // TODO(pmenon): Fill me in

  /**
   * Reset tracker
   */
  void Reset() { allocated_bytes_ = 0; }

  /**
   * @returns number of allocated bytes
   */
  size_t GetAllocatedSize() { return allocated_bytes_; }

  /**
   * Increments number of allocated bytes
   * @param size number to increment by
   */
  void Increment(size_t size) { allocated_bytes_ += size; }

  /**
   * Decrements number of allocated bytes
   * @param size number to decrement by
   */
  void Decrement(size_t size) { allocated_bytes_ -= size; }

 private:
  struct Stats {};
  tbb::enumerable_thread_specific<Stats> stats_;
  // number of bytes allocated
  size_t allocated_bytes_;
};

}  // namespace terrier::execution::sql
