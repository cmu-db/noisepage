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

class GarbageCollector {
 public:
  GarbageCollector() = delete;
  explicit GarbageCollector(transaction::TransactionManager *txn_manager) : txn_manager_(txn_manager), last_run_{0} {}
  ~GarbageCollector() = default;

  std::pair<uint32_t, uint32_t> RunGC() {
    uint32_t garbage_cleared = Deallocate();
    uint32_t txns_cleared = Unlink();
    last_run_ = txn_manager_->GetTimestamp();
    return std::make_pair(garbage_cleared, txns_cleared);
  }

 private:
  transaction::TransactionManager *txn_manager_;
  timestamp_t last_run_;
  // queue of txns that have been unlinked, and should possible be deleted on next GC run
  std::queue<transaction::TransactionContext *> txns_to_deallocate_;
  // queue of txns that need to be unlinked
  std::queue<transaction::TransactionContext *> txns_to_unlink_;

  uint32_t Deallocate() {
    const timestamp_t oldest_txn = txn_manager_->OldestTransactionStartTime();
    uint32_t garbage_cleared = 0;
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

  void UnlinkDeltaRecord(transaction::TransactionContext *const txn, const DeltaRecord &undo_record) {
    DataTable *const table = undo_record.Table();
    const TupleSlot slot = undo_record.Slot();
    const TupleAccessStrategy &accessor = table->accessor_;

    DeltaRecord *version_ptr;
    do {
      version_ptr = table->AtomicallyReadVersionPtr(slot, accessor);
      PELOTON_ASSERT(version_ptr != nullptr, "GC should not be trying to unlink in an empty version chain.");

      if (version_ptr->Timestamp().load() == txn->TxnId()) {
        // our DeltaRecord is the first in the chain, this could get ugly with contention
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
      if (next != nullptr && next->Timestamp().load() == txn->TxnId()) {
        curr->Next().store(next->Next().load());
        break;
      }
    } while (true);
  }

  uint32_t Unlink() {
    const timestamp_t oldest_txn = txn_manager_->OldestTransactionStartTime();
    transaction::TransactionContext *txn = nullptr;

    std::queue<transaction::TransactionContext *> from_txn_manager = txn_manager_->CompletedTransactions();
    if (!txns_to_unlink_.empty()) {
      while (!from_txn_manager.empty()) {
        txn = from_txn_manager.front();
        from_txn_manager.pop();
        txns_to_unlink_.push(txn);
      }
    } else {
      txns_to_unlink_ = from_txn_manager;
    }

    uint32_t txns_cleared = 0;
    std::queue<transaction::TransactionContext *> requeue;
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

    // requeue any txns that we weren't able to unlink yet
    if (!requeue.empty() && !txns_to_unlink_.empty()) {
      while (!requeue.empty()) {
        txn = requeue.front();
        requeue.pop();
        txns_to_unlink_.push(txn);
      }
    } else if (!requeue.empty() && txns_to_unlink_.empty()) {
      txns_to_unlink_ = requeue;
    }

    return txns_cleared;
  }
};

}  // namespace terrier::storage
