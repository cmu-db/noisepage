#pragma once

#include <queue>
#include <utility>
#include "transaction/transaction_context.h"
#include "transaction/transaction_manager.h"

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
   * @param txn_manager pointer to the TransactionManager
   */
  explicit GarbageCollector(transaction::TransactionManager *txn_manager)
      : txn_manager_(txn_manager), last_unlinked_{0} {
    TERRIER_ASSERT(txn_manager_->GCEnabled(),
                   "The TransactionManager needs to be instantiated with gc_enabled true for GC to work!");
  }

  /**
   * Deallocates transactions that can no longer be references by running transactions, and unlinks UndoRecords that
   * are no longer visible to running transactions. This needs to be invoked twice to actually free memory, since the
   * first invocation will unlink a transaction's UndoRecords, while the second time around will allow the GC to free
   * the transaction if safe to do so.
   * @return A pair of numbers: the first is the number of transactions deallocated (deleted) on this iteration, while
   * the second is the number of transactions unlinked on this iteration.
   */
  std::pair<uint32_t, uint32_t> RunGC();

 private:
  /**
   * Process the deallocate queue
   * @return number of txns deallocated (not UndoRecords) for debugging/testing
   */
  uint32_t Deallocate();

  /**
   * Process the unlink queue
   * @return number of txns unlinked (not UndoRecords) for debugging/testing
   */
  uint32_t Unlink();

  /**
   * Given a UndoRecord that has been deemed safe to unlink by the GC, removes it from the version chain. This requires
   * a while loop to handle contention from running transactions (basically restart the process if needed).
   * @param txn pointer to the transaction that created this UndoRecord
   * @param undo_record UndoRecord to be unlinked
   */
  void UnlinkUndoRecord(transaction::TransactionContext *txn, const UndoRecord &undo_record) const;

  transaction::TransactionManager *txn_manager_;
  // timestamp of the last time GC unlinked anything. We need this to know when unlinked versions are safe to deallocate
  timestamp_t last_unlinked_;
  // queue of txns that have been unlinked, and should possible be deleted on next GC run
  std::queue<transaction::TransactionContext *> txns_to_deallocate_;
  // queue of txns that need to be unlinked
  std::queue<transaction::TransactionContext *> txns_to_unlink_;
};

}  // namespace terrier::storage
