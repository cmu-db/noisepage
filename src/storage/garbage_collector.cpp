#include "storage/garbage_collector.h"

#include <unordered_set>
#include <utility>

#include "common/macros.h"
#include "common/thread_context.h"
#include "loggers/storage_logger.h"
#include "metrics/metrics_store.h"
#include "storage/access_observer.h"
#include "storage/data_table.h"
#include "storage/index/index.h"
#include "transaction/deferred_action_manager.h"
#include "transaction/transaction_context.h"
#include "transaction/transaction_util.h"

namespace noisepage::storage {

std::pair<uint32_t, uint32_t> GarbageCollector::PerformGarbageCollection(bool with_limit) {
  if (deferred_action_manager_ != DISABLED) deferred_action_manager_->Process(with_limit);
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

}  // namespace noisepage::storage
