#pragma once

#include "tbb/enumerable_thread_specific.h"

namespace terrier::execution::sql {

/**
 * TODO: track memory usage
 */
class MemoryTracker {
 public:
  // TODO(pmenon): Fill me in

  void Reset() { allocated_bytes_ = 0; }

  size_t GetAllocatedSize() {
    return allocated_bytes_;
  }

  void Increment(size_t size) {
    allocated_bytes_ += size;
  }

  void Decrement(size_t size) {
    allocated_bytes_ -= size;
  }
 private:
  struct Stats {};
  tbb::enumerable_thread_specific<Stats> stats_;
  // number of bytes allocated
  size_t allocated_bytes_;
};

}  // namespace terrier::execution::sql
