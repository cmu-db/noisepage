#pragma once

#include <queue>
#include <unordered_set>
#include <utility>
#include "common/shared_latch.h"
#include "storage/access_observer.h"
#include "transaction/timestamp_manager.h"
#include "transaction/transaction_context.h"
#include "transaction/transaction_defs.h"

#define MAX_OUTSTANDING_UNLINK_TRANSACTIONS 20

namespace terrier::storage {

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
  BOOST_DI_INJECT(GarbageCollector, transaction::TimestampManager *timestamp_manager,
                  transaction::DeferredActionManager *deferred_action_manager, AccessObserver *observer)
      : timestamp_manager_(timestamp_manager),
        deferred_action_manager_(deferred_action_manager),
        observer_(observer),
        last_unlinked_{0} {}

  ~GarbageCollector() = default;

  /**
   * Unlink the transaction and then defers an event to deallocate
   * the transaction context
   * @param the transaction context associated with transaction to unlink
   */
  void CleanupTransaction(transaction::TransactionContext *);

 private:
  void UnlinkTransaction(transaction::timestamp_t oldest_txn, transaction::TransactionContext *txn);

  void ReclaimSlotIfDeleted(UndoRecord *undo_record) const;

  void ReclaimBufferIfVarlen(transaction::TransactionContext *txn, UndoRecord *undo_record) const;

  void TruncateVersionChain(DataTable *table, TupleSlot slot, transaction::timestamp_t oldest) const;

  transaction::TimestampManager *timestamp_manager_;
  transaction::DeferredActionManager *deferred_action_manager_;
  AccessObserver *observer_;
  // timestamp of the last time GC unlinked anything. We need this to know when unlinked versions are safe to deallocate
  transaction::timestamp_t last_unlinked_;
};

}  // namespace terrier::storage
