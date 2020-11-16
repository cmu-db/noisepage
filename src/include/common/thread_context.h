#pragma once

#include "common/managed_pointer.h"
#include "common/resource_tracker.h"

namespace noisepage::metrics {
class MetricsStore;
class MetricsManager;
}  // namespace noisepage::metrics

namespace noisepage::common {

/**
 * thread_local global variables for state needs to be visible to this thread only, and not for sharing state or passing
 * context from one thread to another. Currently envisioned for things like gc_id for the BwTree, and a pointer to this
 * thread's MetricsStore.
 */
struct ThreadContext {
  ~ThreadContext();

  /**
   * nullptr if not registered with MetricsManager
   */
  common::ManagedPointer<metrics::MetricsStore> metrics_store_ = nullptr;

  /**
   * nullptr if not registered with MetricsManager
   */
  ResourceTracker resource_tracker_;
};

/**
 * Define a thread_local ThreadContext for each thread
 */
extern thread_local common::ThreadContext thread_context;

}  // namespace noisepage::common
