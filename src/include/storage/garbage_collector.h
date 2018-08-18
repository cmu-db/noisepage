#pragma once

#include <queue>
#include <utility>
#include "common/container/concurrent_queue.h"
#include "common/macros.h"
#include "loggers/storage_logger.h"
#include "storage/data_table.h"
#include "transaction/transaction_context.h"
#include "transaction/transaction_manager.h"
#include "transaction/transaction_util.h"

namespace terrier::storage {

/**
 * The garbage collector is responsible for processing a queue of completed transactions from the transaction manager.
 * Based on the contents of this queue, it unlinks the DeltaRecords from their version chains when no running
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
  explicit GarbageCollector(transaction::TransactionManager *txn_manager) : txn_manager_(txn_manager), last_run_{0} {
    PELOTON_ASSERT(txn_manager_->GCEnabled(),
                   "The TransactionManager needs to be instantiated with gc_enabled true for GC to work!");
  }

  /**
   * Deallocates transactions that can no longer be references by running transactions, and unlinks DeltaRecords that
   * are no longer visible to running transactions. This needs to be invoked twice to actually free memory, since the
   * first invocation will unlink a transaction's DeltaRecords, while the second time around will allow the GC to free
   * the transaction if safe to do so.
   * @return A pair of numbers: the first is the number of transactions deallocated (deleted) on this iteration, while
   * the second is the number of transactions unlinked on this iteration.
   */
  std::pair<uint64_t, uint32_t> RunGC() {
    uint64_t txns_deallocated = Deallocate();
    uint32_t txns_unlinked = Unlink();
    last_run_ = txn_manager_->GetTimestamp();
    return std::make_pair(txns_deallocated, txns_unlinked);
  }

 private:
  transaction::TransactionManager *txn_manager_;
  // timestamp of the last time GC completed. We need this to know when unlinked versions are safe to deallocate.
  timestamp_t last_run_;
  // queue of txns that have been unlinked, and should possible be deleted on next GC run
  std::queue<transaction::TransactionContext *> txns_to_deallocate_;
  // queue of txns that need to be unlinked
  std::queue<transaction::TransactionContext *> txns_to_unlink_;

  /**
   * Process the deallocate queue
   * @return number of txns deallocated (not DeltaRecords) for debugging/testing
   */
  uint64_t Deallocate() {
    const timestamp_t oldest_txn = txn_manager_->OldestTransactionStartTime();
    uint64_t garbage_cleared = 0;
    transaction::TransactionContext *txn = nullptr;

    if (transaction::TransactionUtil::NewerThan(oldest_txn, last_run_)) {
      // All of the transactions in my deallocation queue were unlinked before the oldest running txn in the system.
      // We are now safe to deallocate these txns because no one should hold a reference to them anymore
      garbage_cleared = txns_to_deallocate_.size();
      while (!txns_to_deallocate_.empty()) {
        txn = txns_to_deallocate_.front();
        txns_to_deallocate_.pop();
        delete txn;
      }
    }

    return garbage_cleared;
  }

  /**
   * Given a DeltaRecord that has been deemed safe to unlink by the GC, removes it from the version chain. This requires
   * a while loop to handle contention from running transactions (basically restart the process if needed).
   * @param txn pointer to the transaction that created this DeltaRecord
   * @param undo_record DeltaRecord to be unlinked
   */
  void UnlinkDeltaRecord(transaction::TransactionContext *const txn, const DeltaRecord &undo_record) {
    PELOTON_ASSERT(txn->TxnId() == undo_record.Timestamp().load(), "This undo_record does not belong to this txn.");
    DataTable *const table = undo_record.Table();
    const TupleSlot slot = undo_record.Slot();
    const TupleAccessStrategy &accessor = table->accessor_;

    DeltaRecord *version_ptr;
    do {
      version_ptr = table->AtomicallyReadVersionPtr(slot, accessor);
      PELOTON_ASSERT(version_ptr != nullptr, "GC should not be trying to unlink in an empty version chain.");

      if (version_ptr->Timestamp().load() == txn->TxnId()) {
        // Our DeltaRecord is the first in the chain, handle contention on the write lock with CAS
        if (table->CompareAndSwapVersionPtr(slot, accessor, version_ptr, version_ptr->Next())) break;
        // Someone swooped the VersionPointer while we were trying to swap it (aka took the write lock)
        version_ptr = table->AtomicallyReadVersionPtr(slot, accessor);
      }
      // a version chain is guaranteed to not change when not at the head (assuming single-threaded GC), so we are safe
      // to traverse and update pointers without CAS
      DeltaRecord *curr = version_ptr;
      DeltaRecord *next = curr->Next();

      // traverse until we hit the DeltaRecord that we want to unlink
      while (next != nullptr && next->Timestamp().load() != txn->TxnId()) {
        curr = next;
        next = curr->Next();
      }
      // we're in position with next being the DeltaRecord to be unlinked
      if (next != nullptr && next->Timestamp().load() == txn->TxnId()) {
        curr->Next().store(next->Next().load());
        break;
      }
      // If that process didn't work (interleaved abort) then try again
    } while (true);
  }

  /**
   * Process the unlink queue
   * @return number of txns unlinked (not DeltaRecords) for debugging/testing
   */
  uint32_t Unlink() {
    const timestamp_t oldest_txn = txn_manager_->OldestTransactionStartTime();
    transaction::TransactionContext *txn = nullptr;

    // Get the completed transactions from the TransactionManager
    std::queue<transaction::TransactionContext *> from_txn_manager = txn_manager_->CompletedTransactions();

    // Append to our local unlink queue
    while (!from_txn_manager.empty()) {
      txn = from_txn_manager.front();
      from_txn_manager.pop();
      txns_to_unlink_.push(txn);
    }

    uint32_t txns_cleared = 0;
    std::queue<transaction::TransactionContext *> requeue;
    // Process every transaction in the unlink queue
    while (!txns_to_unlink_.empty()) {
      txn = txns_to_unlink_.front();
      txns_to_unlink_.pop();
      if (!transaction::TransactionUtil::Committed(txn->TxnId())) {
        // this is an aborted txn. There is nothing to unlink because Rollback() handled that already, but we still need
        // to safely free the txn
        txns_to_deallocate_.push(txn);
        txns_cleared++;
      } else if (transaction::TransactionUtil::NewerThan(oldest_txn, txn->TxnId())) {
        // this is a committed txn that is no visible to any running txns. Proceed with unlinking its DeltaRecords
        transaction::UndoBuffer &undos = txn->GetUndoBuffer();
        for (auto &undo_record : undos) {
          UnlinkDeltaRecord(txn, undo_record);
        }
        txns_to_deallocate_.push(txn);
        txns_cleared++;
      } else {
        // this is a committed txn that is still visible, requeue for next GC run
        requeue.push(txn);
      }
    }

    // requeue any txns that we were still visible to running transactions
    if (!requeue.empty()) {
      txns_to_unlink_ = requeue;
    }

    return txns_cleared;
  }
};

}  // namespace terrier::storage
