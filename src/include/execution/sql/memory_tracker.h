#pragma once

#include <tbb/enumerable_thread_specific.h>

namespace noisepage::execution::sql {

/**
 * Class for tracking memory on a per-thread granularity.
 * Currently tracks allocation size in bytes during thread's execution.
 */
class EXPORT MemoryTracker {
 public:
  /**
   * Reset tracker
   */
  void Reset() { stats_.local().allocated_bytes_ = 0; }

  /**
   * @returns number of allocated bytes
   */
  size_t GetAllocatedSize() { return stats_.local().allocated_bytes_; }

  /**
   * Increments number of allocated bytes
   * @param size number to increment by
   */
  void Increment(size_t size) { stats_.local().allocated_bytes_ += size; }

  /**
   * Decrements number of allocated bytes
   * @param size number to decrement by
   */
  void Decrement(size_t size) { stats_.local().allocated_bytes_ -= size; }

 private:
  /**
   * Struct to store per-thread tracking data.
   */
  struct Stats {
    // Number of bytes allocated
    size_t allocated_bytes_ = 0;
  };
  tbb::enumerable_thread_specific<Stats> stats_;
};

}  // namespace noisepage::execution::sql
