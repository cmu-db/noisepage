#pragma once

#include <queue>
#include <tuple>
#include <unordered_set>
#include <utility>

#include "common/shared_latch.h"
#include "storage/storage_defs.h"
#include "transaction/transaction_defs.h"
#include "transaction/transaction_manager.h"

namespace noisepage::transaction {
class TimestampManager;
class TransactionManager;
class DeferredActionManager;
class TransactionContext;
}  // namespace noisepage::transaction

namespace noisepage::storage {

class AccessObserver;
class DataTable;
class UndoRecord;

namespace index {
class Index;
}

/**
 * The garbage collector is responsible for processing a queue of completed transactions from the transaction manager.
 * Based on the contents of this queue, it unlinks the UndoRecords from their version chains when no running
 * transactions can view those versions anymore. It then stores those transactions to attempt to deallocate on the next
 * iteration if no running transactions can still hold references to them.
 */
class GarbageCollector {
 public:
  /**
   * Constructor for the Garbage Collector that requires a pointer to the TransactionManager. This is necessary for the
   * GC to invoke the TM's function for handing off the completed transactions queue.
   * @param deferred_action_manager pointer to deferred action manager of the system
   * @param txn_manager pointer to the TransactionManager
   */
  // TODO(Tianyu): Eventually the GC will be re-written to be purely on the deferred action manager. which will
  //  eliminate this perceived redundancy of taking in a transaction manager.
  GarbageCollector(const common::ManagedPointer<transaction::DeferredActionManager> deferred_action_manager,
                   const common::ManagedPointer<transaction::TransactionManager> txn_manager)
      : deferred_action_manager_(deferred_action_manager), txn_manager_(txn_manager) {
    NOISEPAGE_ASSERT(txn_manager_->GCEnabled(),
                   "The TransactionManager needs to be instantiated with gc_enabled true for GC to work!");
  }

  ~GarbageCollector() = default;

  /**
   * Deallocates transactions that can no longer be referenced by running transactions, and unlinks UndoRecords that
   * are no longer visible to running transactions. This needs to be invoked twice to actually free memory, since the
   * first invocation will unlink a transaction's UndoRecords, while the second time around will allow the GC to free
   * the transaction if safe to do so. The only exception is read-only transactions, which can be deallocated in a
   * single GC pass.
   * @param with_limit True if for each invocation of this function,
   * there is a upper limit on number of deferred action that can be processed
   * @return A pair of numbers: the first is the number of transactions deallocated (deleted) on this iteration, while
   * the second is the number of transactions unlinked on this iteration.
   */
  std::pair<uint32_t, uint32_t> PerformGarbageCollection(bool with_limit);

  /**
   * Register an index to be periodically garbage collected
   * @param index pointer to the index to register
   */
  void RegisterIndexForGC(common::ManagedPointer<index::Index> index);

  /**
   * Unregister an index to be periodically garbage collected
   * @param index pointer to the index to unregister
   */
  void UnregisterIndexForGC(common::ManagedPointer<index::Index> index);

 private:
  friend class GarbageCollectorThread;
  const common::ManagedPointer<transaction::DeferredActionManager> deferred_action_manager_;
  const common::ManagedPointer<transaction::TransactionManager> txn_manager_;
  // timestamp of the last time GC unlinked anything. We need this to know when unlinked versions are safe to deallocate
};

}  // namespace noisepage::storage
