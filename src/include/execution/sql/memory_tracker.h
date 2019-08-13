#pragma once

#include "tbb/enumerable_thread_specific.h"

namespace terrier::execution::sql {

/**
 * TODO: track memory usage
 */
class MemoryTracker {
 public:
  // TODO(pmenon): Fill me in
 private:
  struct Stats {};
  tbb::enumerable_thread_specific<Stats> stats_;
};

}  // namespace terrier::execution::sql
