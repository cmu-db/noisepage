#pragma once
#include <tbb/reader_writer_lock.h>
#include "common/spin_latch.h"
#include "common/typedefs.h"
#include "storage/data_table.h"
#include "transaction/transaction_context.h"
#include "common/rw_latch.h"

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
   */
  explicit TransactionManager(common::ObjectPool<UndoBufferSegment> *buffer_pool) : buffer_pool_(buffer_pool) {}

  /**
   * Begins a transaction.
   * @return transaction context for the newly begun transaction
   */
  TransactionContext *BeginTransaction() {
    common::ReaderWriterLatch::ScopedReaderLatch guard(&commit_latch_);
    // TODO(Tianyu):
    // Maybe embed this into the data structure, or use an object pool?
    // Doing this with std::map or other data structure is risky though, as they may not
    // guarantee that the iterator or underlying pointer is stable across operations.
    // (That is, they may change as concurrent inserts and deletes happen)
    auto *result = new TransactionContext(time_++, txn_id_++, buffer_pool_);
    table_latch_.Lock();
    auto ret UNUSED_ATTRIBUTE = curr_running_txns_.emplace(result->StartTime(), result);
    PELOTON_ASSERT(ret.second, "commit start time should be globally unique");
    table_latch_.Unlock();
    return result;
  }

  /**
   * Commits a transaction, making all of its changes visible to others.
   * @param txn the transaction to commit
   */
  void Commit(TransactionContext *txn) {
    common::ReaderWriterLatch::ScopedWriterLatch guard(&commit_latch_);
    timestamp_t commit_time = time_++;
    // Flip all timestamps to be committed
    UndoBuffer &undos = txn->GetUndoBuffer();
    for (auto it = undos.Begin(); it != undos.End(); ++it) it->Timestamp().store(commit_time);
    table_latch_.Lock();
    auto ret UNUSED_ATTRIBUTE = curr_running_txns_.erase(txn->StartTime());
    PELOTON_ASSERT(ret == 1, "committed transaction did not exist in global transactions table");
    table_latch_.Unlock();
  }

  /**
   * Aborts a transaction, rolling back its changes (if any).
   * @param txn the transaction to abort.
   */
  void Abort(TransactionContext *txn) {
    // no latch required on undo since all operations are transaction-local
    UndoBuffer &undos = txn->GetUndoBuffer();
    for (auto it = undos.Begin(); it != undos.End(); ++it) it->Table()->Rollback(txn->TxnId(), it->Slot());
    table_latch_.Lock();
    auto ret UNUSED_ATTRIBUTE = curr_running_txns_.erase(txn->StartTime());
    PELOTON_ASSERT(ret == 1, "aborted transaction did not exist in global transactions table");
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
    timestamp_t result = curr_running_txns_.begin()->second->StartTime();
    table_latch_.Unlock();
    return result;
  }

 private:
  common::ObjectPool<UndoBufferSegment> *buffer_pool_;
  // TODO(Tianyu): Timestamp generation needs to be more efficient
  std::atomic<timestamp_t> time_{timestamp_t(0)};
  // TODO(Tianyu): Actually, maybe we can just set txn_id to be start time with the first bit flipped? Start time
  // is globally unique in our setup anyway...
  std::atomic<timestamp_t> txn_id_{timestamp_t(static_cast<uint64_t>(INT64_MIN))};  // start from "negative" value

  // TODO(Tianyu): Maybe don't use tbb?
  // TODO(Tianyu): This is the famed HyPer Latch. We will need to re-evaluate performance later.
  mutable common::ReaderWriterLatch commit_latch_;

  // TODO(Tianyu): Get a better data structure for this.
  // TODO(Tianyu): Also, we are leveraging off the fact that we know start time to be globally unique, so we should
  // think about this when refactoring the txn id thing.
  mutable common::SpinLatch table_latch_;
  std::map<timestamp_t, TransactionContext *> curr_running_txns_;

};
}  // namespace terrier::transaction
