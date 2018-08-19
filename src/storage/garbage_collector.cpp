#include <queue>
#include <utility>
#include "common/container/concurrent_queue.h"
#include "common/macros.h"
#include "loggers/storage_logger.h"
#include "storage/data_table.h"
#include "storage/garbage_collector.h"
#include "transaction/transaction_context.h"
#include "transaction/transaction_manager.h"
#include "transaction/transaction_util.h"

namespace terrier::storage {

std::pair<uint32_t, uint32_t> GarbageCollector::RunGC() {
  uint32_t txns_deallocated = Deallocate();
  uint32_t txns_unlinked = Unlink();
  if (txns_unlinked > 0) {
    last_unlinked_ = txn_manager_->GetTimestamp();
  }
  return std::make_pair(txns_deallocated, txns_unlinked);
}

uint32_t GarbageCollector::Deallocate() {
  const timestamp_t oldest_txn = txn_manager_->OldestTransactionStartTime();
  uint32_t garbage_cleared = 0;
  transaction::TransactionContext *txn = nullptr;

  if (transaction::TransactionUtil::NewerThan(oldest_txn, last_unlinked_)) {
    // All of the transactions in my deallocation queue were unlinked before the oldest running txn in the system.
    // We are now safe to deallocate these txns because no one should hold a reference to them anymore
    garbage_cleared = static_cast<uint32_t>(txns_to_deallocate_.size());
    while (!txns_to_deallocate_.empty()) {
      txn = txns_to_deallocate_.front();
      txns_to_deallocate_.pop();
//      printf("txn deallocated: %llu\n", !txn->StartTime());
      delete txn;
    }
  }

  return garbage_cleared;
}

uint32_t GarbageCollector::Unlink() {
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

  uint32_t txns_unlinked = 0;
  std::queue<transaction::TransactionContext *> requeue;
  // Process every transaction in the unlink queue

  auto before_deallocate_size = txns_to_deallocate_.size();
  while (!txns_to_unlink_.empty()) {
    txn = txns_to_unlink_.front();
    txns_to_unlink_.pop();
    if (!transaction::TransactionUtil::Committed(txn->TxnId())) {
      // this is an aborted txn. There is nothing to unlink because Rollback() handled that already, but we still need
      // to safely free the txn
      txns_to_deallocate_.push(txn);
      txns_unlinked++;
    } else if (transaction::TransactionUtil::NewerThan(oldest_txn, txn->TxnId())) {
      // this is a committed txn that is no visible to any running txns. Proceed with unlinking its DeltaRecords
      transaction::UndoBuffer &undos = txn->GetUndoBuffer();
      for (auto &undo_record : undos) {
        UnlinkDeltaRecord(txn, undo_record);
      }
      txns_to_deallocate_.push(txn);
      txns_unlinked++;
    } else {
      // this is a committed txn that is still visible, requeue for next GC run
      requeue.push(txn);
    }
  }
  // requeue any txns that we were still visible to running transactions
  if (!requeue.empty()) {
    txns_to_unlink_ = requeue;
  }

  return txns_unlinked;
}

void GarbageCollector::UnlinkDeltaRecord(transaction::TransactionContext *const txn,
                                         const DeltaRecord &undo_record) const {
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

}  // namespace terrier::storage
