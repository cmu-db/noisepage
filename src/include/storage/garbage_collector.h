#pragma once

#include <thread>
#include <utility>
#include <vector>
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
  explicit GarbageCollector(transaction::TransactionManager *txn_manager) : txn_manager_(txn_manager) {}
  ~GarbageCollector() = default;

  bool ThreadRunning() const { return running_; }

  std::pair<uint32_t, uint32_t> RunGC() {
    oldest_txn_ = txn_manager_->OldestTransactionStartTime();
    uint32_t garbage_cleared = ClearGarbage();
    uint32_t txns_cleared = ClearTransactions();
    last_run_ = txn_manager_->Time();
    return std::make_pair(garbage_cleared, txns_cleared);
  }

  void StartGCThread() {
    PELOTON_ASSERT(!running_ && gc_thread_ == nullptr, "Should only be invoking this on a GC that is not running.");
    gc_thread_ = new std::thread(&GarbageCollector::ThreadLoop, this);
    running_ = true;
  }

  void StopGCThread() {
    PELOTON_ASSERT(running_ && gc_thread_ != nullptr, "Should only be invoking this on a GC that is running.");
    running_ = false;
    gc_thread_->join();
    delete gc_thread_;
    oldest_txn_ = txn_manager_->OldestTransactionStartTime();
    ClearGarbage();
    ClearTransactions();
    last_run_ = txn_manager_->Time();
    gc_thread_ = nullptr;
  }

 private:
  bool running_ = false;
  transaction::TransactionManager *txn_manager_;
  std::thread *gc_thread_ = nullptr;
  timestamp_t oldest_txn_{0};
  timestamp_t last_run_{0};
  std::vector<transaction::TransactionContext *> garbage_txns_;

  uint32_t ClearGarbage() {
    uint32_t garbage_cleared = 0;
    transaction::TransactionContext *txn = nullptr;
    std::vector<transaction::TransactionContext *> requeue;
    while (!garbage_txns_.empty()) {
      txn = garbage_txns_.back();
      garbage_txns_.pop_back();
      if (transaction::TransactionUtil::NewerThan(last_run_, txn->TxnId())) {
        delete txn;
        garbage_cleared++;
      } else {
        requeue.emplace_back(txn);
      }
    }

    for (auto &i : requeue) {
      garbage_txns_.emplace_back(std::move(i));
    }

    return garbage_cleared;
  }

  uint32_t ClearTransactions() {
    uint32_t txns_cleared = 0;
    uint32_t attempts = 0;
    transaction::TransactionContext *txn = nullptr;
    std::vector<transaction::TransactionContext *> requeue;
    while (txn_manager_->CompletedTransactions().Dequeue(&txn) && attempts < MAX_ATTEMPTS) {
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
            if (table->CompareAndSwapVersionPtr(slot, accessor, curr, next)) {
              // We successfully swapped it
              continue;
            } else {
              // Someone swooped the VersionPointer while we were trying to swap it, start iterating
              curr = table->AtomicallyReadVersionPtr(slot, accessor);
              next = curr->Next();
              PELOTON_ASSERT(next != nullptr,
                             "Somehow we failed the CAS but Next isn't nullptr? That shouldn't happen.");
            }
          } else {
            while (next->Timestamp().load() != txn->TxnId()) {
              curr = next;
              next = curr->Next();
            }
            UNUSED_ATTRIBUTE bool result = curr->Next().compare_exchange_strong(next, next->Next().load());
            PELOTON_ASSERT(result,
                           "We shouldn't be able to fail this CAS later in the version chain since there should be no other writers.");
          }
        }
        garbage_txns_.emplace_back(txn);
        txns_cleared++;
      } else {
        requeue.emplace_back(txn);
      }
    }

    for (auto &i : requeue) {
      txn_manager_->CompletedTransactions().Enqueue(std::move(i));
    }

    return txns_cleared;
  }

  void ThreadLoop() {
    while (running_) {
      if (!txn_manager_->CompletedTransactions().Empty() || !garbage_txns_.empty()) {
        RunGC();
      }
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
  }

};

}  // namespace terrier::storage
