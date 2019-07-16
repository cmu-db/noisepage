#pragma once

#include "common/managed_pointer.h"

namespace terrier::metrics {
class MetricsStore;
class MetricsManager;
}  // namespace terrier::metrics

namespace terrier::common {

/**
 * thread_local global variables for state needs to be visible to this thread only, and not for sharing state or passing
 * context from one thread to another. Currently envisioned for things like gc_id for the BwTree, and a pointer to this
 * thread's MetricsStore.
 */
struct ThreadContext {
  /**
   * @param metrics_store This thread's MetricsStore. Fine to pass in nullptr since registering it with the
   * MetricsManager is what sets the metrics_store_ field.
   */
  explicit ThreadContext(common::ManagedPointer<metrics::MetricsStore> metrics_store,
                         common::ManagedPointer<metrics::MetricsManager> metrics_manager) noexcept
      : metrics_store_(metrics_store), metrics_manager_(metrics_manager) {}

  ~ThreadContext();

  /**
   * nullptr if not registered with MetricsManager
   */
  common::ManagedPointer<metrics::MetricsStore> metrics_store_;

 private:
  /**
   * nullptr if not registered with MetricsManager
   */
  common::ManagedPointer<metrics::MetricsManager> metrics_manager_;
};

extern thread_local common::ThreadContext thread_context;

}  // namespace terrier::common
