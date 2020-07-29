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
std::pair<uint32_t, uint32_t> GarbageCollector::PerformGarbageCollection(bool main_thread) {
  if (deferred_action_manager_ != DISABLED) deferred_action_manager_->Process(main_thread);

  // TODO(John:GC) We should be able to mimic this API interface with counters in the transaction manager that are
  // captured by reference and incremented during the unlink and deallocate actions for the transaction contexts and
  // then queried here.  That may require a resert mechanism and should look to eventually plug into Matt's metrics
  // system.
  return std::make_pair(txn_manager_->NumDeallocated(), txn_manager_->NumUnlinked());
}

void GarbageCollector::RegisterIndexForGC(common::ManagedPointer<index::Index> index) {
  deferred_action_manager_->RegisterIndexForGC(index);
}

/**
 * Unregister an index to be periodically garbage collected
 * @param index pointer to the index to unregister
 */
void GarbageCollector::UnregisterIndexForGC(common::ManagedPointer<index::Index> index) {
  deferred_action_manager_->UnregisterIndexForGC(index);
}

}  // namespace terrier::storage
