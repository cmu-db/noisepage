#pragma once
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
   */
  explicit TransactionManager(common::ObjectPool<UndoBufferSegment> *buffer_pool) : buffer_pool_(buffer_pool) {}

  /**
   * Begins a transaction.
   * @return transaction context for the newly begun transaction
   */
  TransactionContext *BeginTransaction() {
    common::ReaderWriterLatch::ScopedReaderLatch guard(&commit_latch_);
    timestamp_t id = time_++;
    return new TransactionContext{id, id + INT64_MIN, buffer_pool_};
  }

  /**
   * Commits a transaction, making all of its changes visible to others.
   * @param txn the transaction to commit
   */
  timestamp_t Commit(TransactionContext *txn) {
    common::ReaderWriterLatch::ScopedWriterLatch guard(&commit_latch_);
    timestamp_t commit_time = time_++;
    // Flip all timestamps to be committed
    UndoBuffer &undos = txn->GetUndoBuffer();
    for (auto it = undos.Begin(); it != undos.End(); ++it) it->Timestamp().store(commit_time);
    return commit_time;
  }

  /**
   * Aborts a transaction, rolling back its changes (if any).
   * @param txn the transaction to abort.
   */
  void Abort(TransactionContext *txn) {
    // no latch required on undo since all operations are transaction-local
    UndoBuffer &undos = txn->GetUndoBuffer();
    for (auto it = undos.Begin(); it != undos.End(); ++it) it->Table()->Rollback(txn->TxnId(), it->Slot());
  }

 private:
  common::ObjectPool<UndoBufferSegment> *buffer_pool_;
  // TODO(Tianyu): Timestamp generation needs to be more efficient
  std::atomic<timestamp_t> time_{timestamp_t(0)};
//  std::atomic<timestamp_t> txn_id_{timestamp_t(static_cast<uint64_t>(INT64_MIN))};  // start from "negative" value

  // TODO(Tianyu): This is the famed HyPer Latch. We will need to re-evaluate performance later.
  common::ReaderWriterLatch commit_latch_;
};
}  // namespace terrier::transaction
