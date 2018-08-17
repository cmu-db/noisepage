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

static constexpr uint32_t MAX_ATTEMPTS = 1000000;

class GarbageCollector {
 public:
  GarbageCollector() = delete;
  explicit GarbageCollector(transaction::TransactionManager *txn_manager) : txn_manager_(txn_manager), last_run_{0} {}
  ~GarbageCollector() = default;

  std::pair<uint32_t, uint32_t> RunGC() {
    uint32_t garbage_cleared = ClearGarbage();
    uint32_t txns_cleared = ClearTransactions();
    last_run_ = txn_manager_->Time();
    return std::make_pair(garbage_cleared, txns_cleared);
  }

 private:
  transaction::TransactionManager *txn_manager_;
  timestamp_t last_run_;
  // queue of txns that have been unlinked, and should possible be deleted on next GC run
  std::queue<transaction::TransactionContext *> garbage_txns_;
  // queue of txns that need to be unlinked
  std::queue<transaction::TransactionContext *> completed_txns_;

  uint32_t ClearGarbage() {
    uint32_t garbage_cleared = 0;
    transaction::TransactionContext *txn = nullptr;
    std::queue<transaction::TransactionContext *> requeue;
    while (!garbage_txns_.empty()) {
      txn = garbage_txns_.front();
      garbage_txns_.pop();
      if (transaction::TransactionUtil::NewerThan(last_run_, txn->TxnId())) {
        delete txn;
        garbage_cleared++;
      } else {
        requeue.push(txn);
      }
    }

    // requeue any txns that we weren't able to delete yet
    garbage_txns_ = requeue;

    return garbage_cleared;
  }

  uint32_t ClearTransactions() {
    const timestamp_t oldest_txn_ = txn_manager_->OldestTransactionStartTime();
    uint32_t txns_cleared = 0;
    uint32_t attempts = 0;
    transaction::TransactionContext *txn = nullptr;
    std::queue<transaction::TransactionContext *> requeue;
    completed_txns_ = txn_manager_->CompletedTransactions();
    while (!completed_txns_.empty() && attempts < MAX_ATTEMPTS) {
      txn = completed_txns_.front();
      completed_txns_.pop();
      attempts++;
      if (transaction::TransactionUtil::NewerThan(oldest_txn_, txn->TxnId())) {
        transaction::UndoBuffer &undos = txn->GetUndoBuffer();
        for (auto &undo_record : undos) {
          DataTable *const table = undo_record.Table();
          const TupleSlot slot = undo_record.Slot();
          const TupleAccessStrategy &accessor = table->accessor_;
          DeltaRecord *curr = table->AtomicallyReadVersionPtr(slot, accessor);
          DeltaRecord *next = curr->Next();
          if (next == nullptr) {
            PELOTON_ASSERT(curr->Timestamp().load() == txn->TxnId(),
                           "There's only one element in the version chain. This must be our DeltaRecord.");
            if (table->CompareAndSwapVersionPtr(slot, accessor, curr, next)) continue;

            // Someone swooped the VersionPointer while we were trying to swap it, start iterating
            curr = table->AtomicallyReadVersionPtr(slot, accessor);
            next = curr->Next();
            PELOTON_ASSERT(next != nullptr, "Somehow we failed the CAS but Next isn't nullptr? That shouldn't happen.");
          } else {
            while (next->Timestamp().load() != txn->TxnId()) {
              curr = next;
              next = curr->Next();
            }
            UNUSED_ATTRIBUTE bool result = curr->Next().compare_exchange_strong(next, next->Next().load());
            PELOTON_ASSERT(result,
                           "We shouldn't be able to fail this CAS later in the version chain since there should be no "
                           "other writers.");
          }
        }
        garbage_txns_.push(txn);
        txns_cleared++;
      } else {
        requeue.push(txn);
      }
    }

    if (!requeue.empty() && !completed_txns_.empty()) {
      while (!requeue.empty()) {
        txn = requeue.front();
        requeue.pop();
        completed_txns_.push(txn);
      }
    } else if (!requeue.empty() && completed_txns_.empty()) {
      completed_txns_ = requeue;
    }

    return txns_cleared;
  }
};

}  // namespace terrier::storage
