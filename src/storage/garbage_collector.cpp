#include "storage/garbage_collector.h"

#include <unordered_set>
#include <utility>

#include "common/macros.h"
#include "loggers/storage_logger.h"
#include "storage/data_table.h"
#include "transaction/deferred_action_manager.h"
#include "transaction/transaction_context.h"
#include "transaction/transaction_defs.h"
#include "transaction/transaction_manager.h"
#include "transaction/transaction_util.h"

namespace terrier::storage {

// TODO(John:GC) Figure out how we keep the illusion of this for tests but ween the
// main system off this.  Initial thought is we can imitate this with static
// counters that are globally incremented in the callbacks (maybe tunable?).  It
// would be nice if this was hooked into metrics but that's a too far for right
// now.
std::pair<uint32_t, uint32_t> GarbageCollector::PerformGarbageCollection() {
  if (deferred_action_manager_ != DISABLED) deferred_action_manager_->Process();
  ProcessIndexes();

  // TODO(John:GC) We should be able to mimic this API interface with counters in the transaction manager that are
  // captured by reference and incremented during the unlink and deallocate actions for the transaction contexts and
  // then queried here.  That may require a resert mechanism and should look to eventually plug into Matt's metrics
  // system.
  return std::make_pair(txn_manager_->NumDeallocated(), txn_manager_->NumUnlinked());
}

// TODO(John:GC) Where should this live?
void GarbageCollector::RegisterIndexForGC(const common::ManagedPointer<index::Index> index) {
  TERRIER_ASSERT(index != nullptr, "Index cannot be nullptr.");
  common::SharedLatch::ScopedExclusiveLatch guard(&indexes_latch_);
  TERRIER_ASSERT(indexes_.count(index) == 0, "Trying to register an index that has already been registered.");
  indexes_.insert(index);
}

// TODO(John:GC) Where should this live?
void GarbageCollector::UnregisterIndexForGC(const common::ManagedPointer<index::Index> index) {
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
void GarbageCollector::ProcessIndexes() {
  common::SharedLatch::ScopedSharedLatch guard(&indexes_latch_);
  for (const auto &index : indexes_) index->PerformGarbageCollection();
}

}  // namespace terrier::storage
