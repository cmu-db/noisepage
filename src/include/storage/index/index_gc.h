#pragma once
#include <unordered_set>
#include <utility>
#include "common/shared_latch.h"
#include "storage/index/index.h"

namespace terrier::storage::index {
class IndexGC {
 public:
  /**
   * Register an index to be periodically garbage collected
   * @param index pointer to the index to register
   */
  void RegisterIndexForGC(index::Index *index) {
    TERRIER_ASSERT(index != nullptr, "Index cannot be nullptr.");
    common::SharedLatch::ScopedExclusiveLatch guard(&indexes_latch_);
    TERRIER_ASSERT(indexes_.count(index) == 0, "Trying to register an index that has already been registered.");
    indexes_.insert(index);
  }

  /**
   * Unregister an index to be periodically garbage collected
   * @param index pointer to the index to unregister
   */
  void UnregisterIndexForGC(index::Index *index) {
    TERRIER_ASSERT(index != nullptr, "Index cannot be nullptr.");
    common::SharedLatch::ScopedExclusiveLatch guard(&indexes_latch_);
    TERRIER_ASSERT(indexes_.count(index) == 1, "Trying to unregister an index that has not been registered.");
    indexes_.erase(index);
  }

  void Process() {
    common::SharedLatch::ScopedSharedLatch guard(&indexes_latch_);
    for (const auto &index : indexes_) index->PerformGarbageCollection();
  }

 private:
  // For invoking index garbage collectors
  std::unordered_set<index::Index *> indexes_;
  common::SharedLatch indexes_latch_;
};
}  // namespace terrier::storage::index
