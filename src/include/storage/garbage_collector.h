#pragma once

#include <thread>
#include <utility>
#include <vector>
#include "common/container/concurrent_queue.h"
#include "loggers/storage_logger.h"
#include "storage/data_table.h"
#include "transaction/transaction_context.h"
#include "transaction/transaction_manager.h"
#include "transaction/transaction_util.h"

namespace terrier::storage {

class GarbageCollector {
 private:
  bool running_ = false;
  transaction::TransactionManager *txn_manager_;
  std::thread *gc_thread_ = nullptr;
  timestamp_t oldest_txn_{0};
  timestamp_t last_run_{0};
  common::ConcurrentQueue<transaction::TransactionContext *> completed_txns_;
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
    transaction::TransactionContext *txn = nullptr;
    std::vector<transaction::TransactionContext *> requeue;
    while (completed_txns_.Dequeue(&txn)) {
      if (transaction::TransactionUtil::NewerThan(oldest_txn_, txn->TxnId())) {
        transaction::UndoBuffer &undos = txn->GetUndoBuffer();
        for (UNUSED_ATTRIBUTE auto &undo_record : undos) {
          // prune version chain
        }
        garbage_txns_.emplace_back(txn);
        txns_cleared++;
      } else {
        requeue.emplace_back(txn);
      }
    }

    for (auto &i : requeue) {
      completed_txns_.Enqueue(std::move(i));
    }

    return txns_cleared;
  }

  void ThreadLoop() {
    while (running_) {
      STORAGE_LOG_INFO("hello from a GC thread\n");
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
  }

  void RunGC() {
    ClearGarbage();
    ClearTransactions();
    last_run_ = txn_manager_->Time();
  }

 public:
  GarbageCollector() = delete;
  explicit GarbageCollector(transaction::TransactionManager *txn_manager) : txn_manager_(txn_manager) {}
  ~GarbageCollector() = default;

  bool Running() const {
    return running_;
  }

  void StartGC() {
    PELOTON_ASSERT(!running_ && gc_thread_ == nullptr, "Should only be invoking this on a GC that is not running.");
    gc_thread_ = new std::thread(&GarbageCollector::ThreadLoop, this);
    running_ = true;
  }

  void StopGC() {
    PELOTON_ASSERT(running_ && gc_thread_ != nullptr, "Should only be invoking this on a GC that is running.");
    running_ = false;
    gc_thread_->join();
    gc_thread_ = nullptr;
  }

 public:
  void AddGarbage(transaction::TransactionContext *txn) { completed_txns_.Enqueue(std::move(txn)); }
};

}  // namespace terrier::storage
