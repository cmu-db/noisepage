#pragma once
#include <map>
#include <queue>
#include <utility>
#include "common/rw_latch.h"
#include "common/spin_latch.h"
#include "common/typedefs.h"
#include "storage/data_table.h"
#include "transaction/transaction_context.h"

namespace terrier::transaction {
/**
 * A transaction manager maintains global state about all running transactions, and is responsible for creating,
 * committing and aborting transactions
 */
class TransactionManager {
  // TODO(Tianyu): Implement the global transaction tables
 public:
  /**
   * Initializes a new transaction manager. Transactions will use the given object pool as source of their undo
   * buffers.
   * @param buffer_pool the buffer pool to use for transaction undo buffers
   * @param gc_enabled true if txns should be stored in a local queue to hand off to the GC, false otherwise
   */
  explicit TransactionManager(common::ObjectPool<UndoBufferSegment> *buffer_pool, bool gc_enabled)
      : buffer_pool_(buffer_pool), gc_enabled_(gc_enabled) {}

  /**
   * Begins a transaction.
   * @return transaction context for the newly begun transaction
   */
  TransactionContext *BeginTransaction() {
    common::ReaderWriterLatch::ScopedReaderLatch guard(&commit_latch_);
    timestamp_t id = time_++;
    // TODO(Tianyu):
    // Maybe embed this into the data structure, or use an object pool?
    // Doing this with std::map or other data structure is risky though, as they may not
    // guarantee that the iterator or underlying pointer is stable across operations.
    // (That is, they may change as concurrent inserts and deletes happen)
    auto *result = new TransactionContext(id, id + INT64_MIN, buffer_pool_);
    table_latch_.Lock();
    auto ret UNUSED_ATTRIBUTE = curr_running_txns_.emplace(result->StartTime(), result);
    PELOTON_ASSERT(ret.second, "commit start time should be globally unique");
    table_latch_.Unlock();
    return result;
  }

  /**
   * Commits a transaction, making all of its changes visible to others.
   * @param txn the transaction to commit
   * @return commit timestamp of this transaction
   */
  timestamp_t Commit(TransactionContext *txn) {
    common::ReaderWriterLatch::ScopedWriterLatch guard(&commit_latch_);
    const timestamp_t commit_time = time_++;
    // Flip all timestamps to be committed
    UndoBuffer &undos = txn->GetUndoBuffer();
    for (auto &it : undos) it.Timestamp().store(commit_time);
    table_latch_.Lock();
    const timestamp_t start_time = txn->StartTime();
    size_t result UNUSED_ATTRIBUTE = curr_running_txns_.erase(start_time);
    PELOTON_ASSERT(result == 1, "committed transaction did not exist in global transactions table");
    txn->TxnId() = commit_time;
    if (gc_enabled_) completed_txns_.push(txn);
    table_latch_.Unlock();
    return commit_time;
  }

  /**
   * Aborts a transaction, rolling back its changes (if any).
   * @param txn the transaction to abort.
   */
  void Abort(TransactionContext *txn) {
    // no latch required on undo since all operations are transaction-local
    UndoBuffer &undos = txn->GetUndoBuffer();
    for (auto &it : undos) it.Table()->Rollback(txn->TxnId(), it.Slot());
    table_latch_.Lock();
    const timestamp_t start_time = txn->StartTime();
    size_t ret UNUSED_ATTRIBUTE = curr_running_txns_.erase(start_time);
    PELOTON_ASSERT(ret == 1, "aborted transaction did not exist in global transactions table");
    if (gc_enabled_) completed_txns_.push(txn);
    table_latch_.Unlock();
  }

  /**
   * Get the oldest transaction alive in the system at this time. Because of concurrent operations, it
   * is not guaranteed that upon return the txn is still alive. However, it is guaranteed that the return
   * timestamp is older than any transactions live.
   * @return timestamp that is older than any transactions alive
   */
  timestamp_t OldestTransactionStartTime() const {
    table_latch_.Lock();
    auto oldest_txn = curr_running_txns_.begin();
    timestamp_t result = (oldest_txn != curr_running_txns_.end()) ? oldest_txn->second->StartTime() : time_.load();
    table_latch_.Unlock();
    return result;
  }

  /**
   * @return unique timestamp based on current time, and advances one tick
   */
  timestamp_t GetTimestamp() { return time_++; }

  /**
   * @return true if gc_enabled and storing completed txns in local queue, false otherwise
   */
  bool GCEnabled() const { return gc_enabled_; }

  /**
   * Return the completed txns queue and empty it
   * @return
   */
  std::queue<transaction::TransactionContext *> CompletedTransactions() {
    table_latch_.Lock();
    std::queue<transaction::TransactionContext *> hand_to_gc(std::move(completed_txns_));
    PELOTON_ASSERT(completed_txns_.empty(), "TransactionManager's queue should now be empty.");

    table_latch_.Unlock();
    return hand_to_gc;
  }

 private:
  common::ObjectPool<UndoBufferSegment> *buffer_pool_;
  // TODO(Tianyu): Timestamp generation needs to be more efficient
  // TODO(Tianyu): We don't handle timestamp wrap-arounds. I doubt this would be an issue though.
  std::atomic<timestamp_t> time_{timestamp_t(0)};

  // TODO(Tianyu): Maybe don't use tbb?
  // TODO(Tianyu): This is the famed HyPer Latch. We will need to re-evaluate performance later.
  common::ReaderWriterLatch commit_latch_;

  // TODO(Tianyu): Get a better data structure for this.
  // TODO(Tianyu): Also, we are leveraging off the fact that we know start time to be globally unique, so we should
  // think about this when refactoring the txn id thing.
  mutable common::SpinLatch table_latch_;
  std::map<timestamp_t, TransactionContext *> curr_running_txns_;

  bool gc_enabled_ = false;
  std::queue<transaction::TransactionContext *> completed_txns_;
};
}  // namespace terrier::transaction
