#pragma once

#include <queue>
#include <utility>
#include <vector>
#include "transaction/transaction_context.h"
#include "transaction/transaction_defs.h"
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
   * Deallocates transactions that can no longer be referenced by running transactions, and unlinks UndoRecords that
   * are no longer visible to running transactions. This needs to be invoked twice to actually free memory, since the
   * first invocation will unlink a transaction's UndoRecords, while the second time around will allow the GC to free
   * the transaction if safe to do so. The only exception is read-only transactions, which can be deallocated in a
   * single GC pass.
   * @return A pair of numbers: the first is the number of transactions deallocated (deleted) on this iteration, while
   * the second is the number of transactions unlinked on this iteration.
   */
  std::pair<uint32_t, uint32_t> PerformGarbageCollection();

 private:
  /**
   * Process the deallocate queue
   * @return number of txns (not UndoRecords) processed for debugging/testing
   */
  uint32_t ProcessDeallocateQueue();

  /**
   * Process the unlink queue
   * @return number of txns (not UndoRecords) processed for debugging/testing
   */
  uint32_t ProcessUnlinkQueue();

  bool ProcessUndoRecord(transaction::TransactionContext *txn, UndoRecord *undo_record,
                         std::vector<transaction::timestamp_t> *active_txns) const;

  void ReclaimSlotIfDeleted(UndoRecord *undo_record) const;

  void ReclaimBufferIfVarlen(transaction::TransactionContext *txn, UndoRecord *undo_record) const;
  /**
   * Given a UndoRecord that has been deemed safe to unlink by the GC, attempts to remove it from the version chain.
   * It's possible that this process will fail because the GC is conservative with conflicts. If the UndoRecord in the
   * version chain to be updated in order to unlink the target UndoRecord is not yet committed, we will fail and
   * expect this txn to be requeued and we'll try again on the next GC invocation, hopefully after the conflicting txn
   * is either committed or aborted.
   * @param txn pointer to the transaction that created this UndoRecord
   * @param undo_record UndoRecord to be unlinked
   * @return true if the UndoRecord was either unlinked successfully or already unlinked, false otherwise
   */
  bool UnlinkUndoRecord(transaction::TransactionContext *txn, UndoRecord *undo_record,
                        std::vector<transaction::timestamp_t> *active_txns) const;

  /**
   * Given a version chain, perform interval gc on all versions except the head of the chain
   * @param txn pointer to the transaction that created an UndoRecord in this chain
   * @param version_chain_head pointer to the head of the chain
   * @param active_txns vector containing all active transactions
   * @return true if an UndoRecord created by txn was unlinked
   */
  bool UnlinkUndoRecordRestOfChain(transaction::TransactionContext *txn, UndoRecord *version_chain_head,
                                   std::vector<transaction::timestamp_t> *active_txns) const;

  bool UnlinkUndoRecordHead(transaction::TransactionContext *const txn, UndoRecord *const head,
                            std::vector<transaction::timestamp_t> *const active_txns) const;
    /**
   * Straight up unlink the undo_record and reclaim its space
   * @param txn
   * @param undo_record
   */
  void UnlinkUndoRecordVersion(transaction::TransactionContext *txn, UndoRecord *undo_record) const;

  transaction::TransactionManager *const txn_manager_;
  // timestamp of the last time GC unlinked anything. We need this to know when unlinked versions are safe to deallocate
  transaction::timestamp_t last_unlinked_;
  // queue of txns that have been unlinked, and should possible be deleted on next GC run
  transaction::TransactionQueue txns_to_deallocate_;
  // queue of txns that need to be unlinked
  transaction::TransactionQueue txns_to_unlink_;
};

}  // namespace terrier::storage
