#include "transaction/deferred_action_manager.h"

#include "common/macros.h"

namespace terrier::transaction {

void DeferredActionManager::RegisterIndexForGC(const common::ManagedPointer<storage::index::Index> index) {
  TERRIER_ASSERT(index != nullptr, "Index cannot be nullptr.");
  common::SharedLatch::ScopedExclusiveLatch guard(&indexes_latch_);
  TERRIER_ASSERT(indexes_.count(index) == 0, "Trying to register an index that has already been registered.");
  indexes_.insert(index);
}

void DeferredActionManager::UnregisterIndexForGC(const common::ManagedPointer<storage::index::Index> index) {
  TERRIER_ASSERT(index != nullptr, "Index cannot be nullptr.");
  common::SharedLatch::ScopedExclusiveLatch guard(&indexes_latch_);
  TERRIER_ASSERT(indexes_.count(index) == 1, "Trying to unregister an index that has not been registered.");
  indexes_.erase(index);
}

// TODO(John:GC) What is the right way to invoke this?  Should it be a periodic task in the deferred action queue or
// special cased (which feels dirty)?  Frequency of call should not affect index performance just memory usage.  In
// fact, keeping transaction processing regulare should be more important for throughput than calling this.  Also, this
// potentially introduces a long pause in normal processing (could be bad) and could block (or be blocked by) DDL.
//
// @bug This should be exclusive because concurrent calls to PerformGarbageCollection for the same index may not be
// thread safe (i.e. bwtrees...)
void DeferredActionManager::ProcessIndexes() {
  common::SharedLatch::ScopedSharedLatch guard(&indexes_latch_);
  for (const auto &index : indexes_) index->PerformGarbageCollection();
}

}  // namespace terrier::transaction
