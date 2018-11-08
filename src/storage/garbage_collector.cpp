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
  STORAGE_LOG_TRACE("GarbageCollector::PerformGarbageCollection(): txns_deallocated: {}", txns_deallocated);
  uint32_t txns_unlinked = ProcessUnlinkQueue();
  STORAGE_LOG_TRACE("GarbageCollector::PerformGarbageCollection(): txns_unlinked: {}", txns_unlinked);
  if (txns_unlinked > 0) {
    // Only update this field if we actually unlinked anything, otherwise we're being too conservative about when it's
    // safe to deallocate the transactions in our queue.
    last_unlinked_ = txn_manager_->GetTimestamp();
  }
  STORAGE_LOG_TRACE("GarbageCollector::PerformGarbageCollection(): last_unlinked_: {}",
                    static_cast<uint64_t>(last_unlinked_));
  return std::make_pair(txns_deallocated, txns_unlinked);
}

uint32_t GarbageCollector::ProcessDeallocateQueue() {
  const transaction::timestamp_t oldest_txn = txn_manager_->OldestTransactionStartTime();
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
  const transaction::timestamp_t oldest_txn = txn_manager_->OldestTransactionStartTime();
  transaction::TransactionContext *txn = nullptr;

  // Get the completed transactions from the TransactionManager
  transaction::TransactionQueue completed_txns = txn_manager_->CompletedTransactionsForGC();
  if (!completed_txns.empty()) {
    // Append to our local unlink queue
    txns_to_unlink_.splice_after(txns_to_unlink_.cbefore_begin(), std::move(completed_txns));
  }

  uint32_t txns_processed = 0;
  transaction::TransactionQueue requeue;
  // Process every transaction in the unlink queue

  while (!txns_to_unlink_.empty()) {
    txn = txns_to_unlink_.front();
    txns_to_unlink_.pop_front();
    if (txn->undo_buffer_.Empty()) {
      // This is a read-only transaction so this is safe to immediately delete
      delete txn;
      txns_processed++;
    } else if (!transaction::TransactionUtil::Committed(txn->TxnId().load())) {
      // This is an aborted txn. There is nothing to unlink because Rollback() handled that already, but we still need
      // to safely free the txn
      txns_to_deallocate_.push_front(txn);
      txns_processed++;
    } else if (transaction::TransactionUtil::NewerThan(oldest_txn, txn->TxnId().load())) {
      // This is a committed txn that is not visible to any running txns. Proceed with unlinking its UndoRecords

      bool all_unlinked = true;
      for (auto &undo_record : txn->undo_buffer_) {
        all_unlinked = all_unlinked && UnlinkUndoRecord(txn, &undo_record);
      }
      if (all_unlinked) {
        // We unlinked all of the UndoRecords for this txn, so we can add it to the deallocation queue
        txns_to_deallocate_.push_front(txn);
        txns_processed++;
      } else {
        // We didn't unlink all of the UndoRecords (UnlinkUndoRecord returned false due to a write-write conflict),
        // requeue txn for next GC run. Unlinked UndoRecords will be skipped on the next time around since we use the
        // table pointer of an UndoRecord as the internal marker of being unlinked or not
        requeue.push_front(txn);
      }
    } else {
      // This is a committed txn that is still visible, requeue for next GC run
      requeue.push_front(txn);
    }
  }

  // Requeue any txns that we were still visible to running transactions
  if (!requeue.empty()) {
    txns_to_unlink_ = transaction::TransactionQueue(std::move(requeue));
  }

  return txns_processed;
}

bool GarbageCollector::UnlinkUndoRecord(transaction::TransactionContext *const txn,
                                        UndoRecord *const undo_record) const {
  TERRIER_ASSERT(txn->TxnId().load() == undo_record->Timestamp().load(),
                 "This undo_record does not belong to this txn.");
  DataTable *table = undo_record->Table();
  if (table == nullptr) {
    // This UndoRecord has already been unlinked, so we can skip it
    return true;
  }
  const TupleSlot slot = undo_record->Slot();
  const TupleAccessStrategy &accessor = table->accessor_;

  UndoRecord *version_ptr;
  version_ptr = table->AtomicallyReadVersionPtr(slot, accessor);
  TERRIER_ASSERT(version_ptr != nullptr, "GC should not be trying to unlink in an empty version chain.");

  if (version_ptr->Timestamp().load() == txn->TxnId().load()) {
    // Our UndoRecord is the first in the chain, handle contention on the write lock with CAS
    if (table->CompareAndSwapVersionPtr(slot, accessor, version_ptr, version_ptr->Next())) {
      // Mark this UndoRecord as unlinked from the version chain by setting the table pointer to nullptr.
      undo_record->Table() = nullptr;
      return true;
    }
    // Someone swooped the VersionPointer while we were trying to swap it (aka took the write lock)
    version_ptr = table->AtomicallyReadVersionPtr(slot, accessor);
  }
  // a version chain is guaranteed to not change when not at the head (assuming single-threaded GC), so we are safe
  // to traverse and update pointers without CAS
  UndoRecord *curr = version_ptr;
  UndoRecord *next;

  // Traverse until we find the UndoRecord that we want to unlink
  while (true) {
    next = curr->Next();
    TERRIER_ASSERT(next != nullptr, "record to unlink is not found");
    if (next->Timestamp().load() == txn->TxnId().load()) break;
    curr = next;
  }

  // We're in position with next being the UndoRecord to be unlinked, check if curr is committed to avoid contending
  // with interleaved aborts. This is essentially applying first writer wins to the GC's logic.
  if (transaction::TransactionUtil::Committed(curr->Timestamp().load())) {
    // Update the next pointer to unlink the UndoRecord
    curr->Next().store(next->Next().load());
    // Mark this UndoRecord as unlinked from the version chain by setting the table pointer to nullptr.
    undo_record->Table() = nullptr;
    return true;
  }

  // We did not successfully unlink this UndoRecord (due to the curr UndoRecord that we wanted to update to unlink our
  // target UndoRecord not yet being committed)
  return false;
}

}  // namespace terrier::storage
