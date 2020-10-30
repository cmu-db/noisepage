#pragma once
#include <queue>
#include <unordered_set>

#include "common/managed_pointer.h"
#include "common/resource_tracker.h"
#include "storage/storage_defs.h"

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

  /**
   * It is sufficient to truncate each version chain once in a GC invocation because we only read the maximal safe
   * timestamp once, and the version chain is sorted by timestamp. Here we keep a set of slots to truncate to avoid
   * wasteful traversals of the version chain.
   */
  std::unordered_set<storage::TupleSlot> visited_slots_;

  /**
   * Record number of transactions processed by this thread. Used by transaction manager to decide
   * if this thread will cooperatively clean up the deferred action queue after current transaction completion.
   */
  uint32_t num_txns_completed_{0};
};

/**
 * Define a thread_local ThreadContext for each thread
 */
extern thread_local common::ThreadContext thread_context;

}  // namespace noisepage::common
