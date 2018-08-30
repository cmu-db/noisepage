#include "storage/garbage_collector.h"
#include <utility>
#include "common/container/concurrent_queue.h"
#include "common/macros.h"
#include "loggers/storage_logger.h"
#include "storage/data_table.h"
#include "transaction/transaction_context.h"
#include "transaction/transaction_defs.h"
#include "transaction/transaction_manager.h"
#include "transaction/transaction_util.h"

namespace terrier::storage {

std::pair<uint32_t, uint32_t> GarbageCollector::PerformGarbageCollection() {
  uint32_t txns_deallocated = ProcessDeallocateQueue();
  uint32_t txns_unlinked = ProcessUnlinkQueue();
  if (txns_unlinked > 0) {
    // Only update this field if we actually unlinked anything, otherwise we're being too conservative about when it's
    // safe to deallocate the transactions in our queue.
    last_unlinked_ = txn_manager_->GetTimestamp();
  }
  STORAGE_LOG_TRACE("GarbageCollector::PerformGarbageCollection(): last_unlinked_: {}",
                    static_cast<uint64_t>(last_unlinked_));
  STORAGE_LOG_TRACE("GarbageCollector::PerformGarbageCollection(): txns_deallocated: {}", txns_deallocated);
  STORAGE_LOG_TRACE("GarbageCollector::PerformGarbageCollection(): txns_unlinked: {}", txns_unlinked);
  return std::make_pair(txns_deallocated, txns_unlinked);
}

uint32_t GarbageCollector::ProcessDeallocateQueue() {
  const timestamp_t oldest_txn = txn_manager_->OldestTransactionStartTime();
  uint32_t txns_processed = 0;
  transaction::TransactionContext *txn = nullptr;

  if (transaction::TransactionUtil::NewerThan(oldest_txn, last_unlinked_)) {
    // All of the transactions in my deallocation queue were unlinked before the oldest running txn in the system.
    // We are now safe to deallocate these txns because no one should hold a reference to them anymore
    while (!txns_to_deallocate_.empty()) {
      txn = txns_to_deallocate_.front();
      txns_to_deallocate_.pop_front();
      delete txn;
      txns_processed++;
    }
  }

  return txns_processed;
}

uint32_t GarbageCollector::ProcessUnlinkQueue() {
  const timestamp_t oldest_txn = txn_manager_->OldestTransactionStartTime();
  transaction::TransactionContext *txn = nullptr;

  // Get the completed transactions from the TransactionManager
  transaction::TransactionQueue completed_txns = txn_manager_->CompletedTransactionsForGC();
  if (!completed_txns.empty()) {
    // Append to our local unlink queue
    txns_to_unlink_.splice_after(txns_to_unlink_.cbefore_begin(), std::move(completed_txns));
  }
  TERRIER_ASSERT(completed_txns.empty(), "Queue from TransactionManager should now be empty.");

  uint32_t txns_processed = 0;
  transaction::TransactionQueue requeue;
  // Process every transaction in the unlink queue

  while (!txns_to_unlink_.empty()) {
    txn = txns_to_unlink_.front();
    txns_to_unlink_.pop_front();
    if (txn->GetUndoBuffer().Empty()) {
      // this is a read-only transaction so this is safe to immediately delete
      delete txn;
      txns_processed++;
    } else if (!transaction::TransactionUtil::Committed(txn->TxnId().load())) {
      // this is an aborted txn. There is nothing to unlink because Rollback() handled that already, but we still need
      // to safely free the txn
      txns_to_deallocate_.push_front(txn);
      txns_processed++;
    } else if (transaction::TransactionUtil::NewerThan(oldest_txn, txn->TxnId().load())) {
      // this is a committed txn that is no visible to any running txns. Proceed with unlinking its UndoRecords
      UndoBuffer &undos = txn->GetUndoBuffer();
      for (auto &undo_record : undos) {
        UnlinkUndoRecord(txn, undo_record);
      }
      txns_to_deallocate_.push_front(txn);
      txns_processed++;
    } else {
      // this is a committed txn that is still visible, requeue for next GC run
      requeue.push_front(txn);
    }
  }

  // requeue any txns that we were still visible to running transactions
  if (!requeue.empty()) {
    txns_to_unlink_ = transaction::TransactionQueue(std::move(requeue));
  }

  return txns_processed;
}

void GarbageCollector::UnlinkUndoRecord(transaction::TransactionContext *const txn,
                                        const UndoRecord &undo_record) const {
  TERRIER_ASSERT(txn->TxnId().load() == undo_record.Timestamp().load(),
                 "This undo_record does not belong to this txn.");
  DataTable *const table = undo_record.Table();
  const TupleSlot slot = undo_record.Slot();
  const TupleAccessStrategy &accessor = table->accessor_;

  UndoRecord *version_ptr;
  do {
    version_ptr = table->AtomicallyReadVersionPtr(slot, accessor);
    TERRIER_ASSERT(version_ptr != nullptr, "GC should not be trying to unlink in an empty version chain.");

    if (version_ptr->Timestamp().load() == txn->TxnId().load()) {
      // Our UndoRecord is the first in the chain, handle contention on the write lock with CAS
      if (table->CompareAndSwapVersionPtr(slot, accessor, version_ptr, version_ptr->Next())) break;
      // Someone swooped the VersionPointer while we were trying to swap it (aka took the write lock)
      version_ptr = table->AtomicallyReadVersionPtr(slot, accessor);
    }
    // a version chain is guaranteed to not change when not at the head (assuming single-threaded GC), so we are safe
    // to traverse and update pointers without CAS
    UndoRecord *curr = version_ptr;
    UndoRecord *next = curr->Next();

    // traverse until we hit the UndoRecord that we want to unlink
    while (next != nullptr && next->Timestamp().load() != txn->TxnId().load()) {
      curr = next;
      next = curr->Next();
    }
    // we're in position with next being the UndoRecord to be unlinked
    if (next != nullptr && next->Timestamp().load() == txn->TxnId().load()) {
      curr->Next().store(next->Next().load());
      break;
    }
    // If that process didn't work (interleaved abort) then try again
  } while (true);
}

}  // namespace terrier::storage
