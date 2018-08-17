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
    last_run_ = txn_manager_->Time();
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
    uint32_t garbage_cleared = 0;
    transaction::TransactionContext *txn = nullptr;
    std::queue<transaction::TransactionContext *> requeue;
    while (!txns_to_deallocate_.empty()) {
      txn = txns_to_deallocate_.front();
      txns_to_deallocate_.pop();
      if (transaction::TransactionUtil::NewerThan(last_run_, txn->TxnId())) {
        delete txn;
        garbage_cleared++;
      } else {
        requeue.push(txn);
      }
    }

    // requeue any txns that we weren't able to deallocate yet
    txns_to_deallocate_ = requeue;

    return garbage_cleared;
  }

  void UnlinkDeltaRecord(transaction::TransactionContext *const txn, const DeltaRecord &undo_record) {

    DataTable *const table = undo_record.Table();
    const TupleSlot slot = undo_record.Slot();
    const TupleAccessStrategy &accessor = table->accessor_;
    DeltaRecord *curr = table->AtomicallyReadVersionPtr(slot, accessor);
    DeltaRecord *next = curr->Next();
    if (next == nullptr) {
      PELOTON_ASSERT(curr->Timestamp().load() == txn->TxnId(),
                     "There's only one element in the version chain. This must be our DeltaRecord.");
      if (table->CompareAndSwapVersionPtr(slot, accessor, curr, next)) return;

      // Someone swooped the VersionPointer while we were trying to swap it
      curr = table->AtomicallyReadVersionPtr(slot, accessor);
      next = curr->Next();
      // TODO: an abort on another txn would screw this assumption up
      PELOTON_ASSERT(next != nullptr, "Somehow we failed the CAS but Next isn't nullptr? That shouldn't happen.");
    } else {
      while (next->Timestamp().load() != txn->TxnId()) {
        curr = next;
        next = curr->Next();
      }
      curr->Next().store(next->Next().load());
    }
  }

  uint32_t Unlink() {
    const timestamp_t oldest_txn_ = txn_manager_->OldestTransactionStartTime();
    // TODO need to add to it, not just overwrite
    txns_to_unlink_ = txn_manager_->CompletedTransactions();
    uint32_t txns_cleared = 0;
    transaction::TransactionContext *txn = nullptr;
    std::queue<transaction::TransactionContext *> requeue;
    txns_to_unlink_ = txn_manager_->CompletedTransactions();
    while (!txns_to_unlink_.empty()) {
      txn = txns_to_unlink_.front();
      txns_to_unlink_.pop();
      if (transaction::TransactionUtil::NewerThan(oldest_txn_, txn->TxnId())) {
        transaction::UndoBuffer &undos = txn->GetUndoBuffer();
        for (auto &undo_record : undos) {
          UnlinkDeltaRecord(txn, undo_record);
        }
        txns_to_deallocate_.push(txn);
        txns_cleared++;
      } else {
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
