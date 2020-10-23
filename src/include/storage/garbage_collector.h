#pragma once

#include <queue>
#include <tuple>
#include <unordered_set>
#include <utility>

#include "common/shared_latch.h"
#include "storage/storage_defs.h"
#include "transaction/transaction_defs.h"

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
   * @param timestamp_manager source of timestamps in the system
   * @param deferred_action_manager pointer to deferred action manager of the system
   * @param txn_manager pointer to the TransactionManager
   * @param observer the access observer attached to this GC. The GC reports every record gc-ed to the observer if
   *                 it is not null. The observer can then gain insight invoke other components to perform actions.
   *                 The observer's function implementation needs to be lightweight because it is called on the GC
   *                 thread.
   */
  // TODO(Tianyu): Eventually the GC will be re-written to be purely on the deferred action manager. which will
  //  eliminate this perceived redundancy of taking in a transaction manager.
  GarbageCollector(common::ManagedPointer<transaction::TimestampManager> timestamp_manager,
                   common::ManagedPointer<transaction::DeferredActionManager> deferred_action_manager,
                   common::ManagedPointer<transaction::TransactionManager> txn_manager, AccessObserver *observer);

  ~GarbageCollector() {
    NOISEPAGE_ASSERT(txns_to_deallocate_.empty(), "Not all txns have been deallocated");
    NOISEPAGE_ASSERT(txns_to_unlink_.empty(), "Not all txns have been unlinked");
  }

  /**
   * Deallocates transactions that can no longer be referenced by running transactions, and unlinks UndoRecords that
   * are no longer visible to running transactions. This needs to be invoked twice to actually free memory, since the
   * first invocation will unlink a transaction's UndoRecords, while the second time around will allow the GC to free
   * the transaction if safe to do so. The only exception is read-only transactions, which can be deallocated in a
   * single GC pass.
   * @return A pair of numbers: the first is the number of transactions deallocated (deleted) on this iteration, while
   * the second is the number of transactions unlinked on this iteration.
   */
  std::pair<uint32_t, uint32_t> PerformGarbageCollection();

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

  /**
   * Set the GC interval for metrics collection
   * TODO(lma): this need to be called in the settings callback after we add the ability to change the GC interval
   * @param gc_interval interval to set (in us)
   */
  void SetGCInterval(uint64_t gc_interval) { gc_interval_ = gc_interval; }

 private:
  /**
   * Process the deallocate queue
   * @return number of txns (not UndoRecords) processed for debugging/testing
   */
  uint32_t ProcessDeallocateQueue(transaction::timestamp_t oldest_txn);

  /**
   * Process the unlink queue
   * @return a tuple
   *   first element - number of txns processed
   *   second element - number UndoRecords processed
   *   first element - number of read-only txns processed
   */
  std::tuple<uint32_t, uint32_t, uint32_t> ProcessUnlinkQueue(transaction::timestamp_t oldest_txn);

  /**
   * Process deferred actions
   */
  void ProcessDeferredActions(transaction::timestamp_t oldest_txn);

  void ReclaimSlotIfDeleted(UndoRecord *undo_record) const;

  void ReclaimBufferIfVarlen(transaction::TransactionContext *txn, UndoRecord *undo_record) const;

  void TruncateVersionChain(DataTable *table, TupleSlot slot, transaction::timestamp_t oldest) const;

  void ProcessIndexes();

  const common::ManagedPointer<transaction::TimestampManager> timestamp_manager_;
  const common::ManagedPointer<transaction::DeferredActionManager> deferred_action_manager_;
  const common::ManagedPointer<transaction::TransactionManager> txn_manager_;
  AccessObserver *observer_;
  // timestamp of the last time GC unlinked anything. We need this to know when unlinked versions are safe to deallocate
  transaction::timestamp_t last_unlinked_;
  // queue of txns that have been unlinked, and should possible be deleted on next GC run
  transaction::TransactionQueue txns_to_deallocate_;
  // queue of txns that need to be unlinked
  transaction::TransactionQueue txns_to_unlink_;

  std::unordered_set<common::ManagedPointer<index::Index>> indexes_;
  common::SharedLatch indexes_latch_;

  uint64_t gc_interval_{0};
};

}  // namespace noisepage::storage
