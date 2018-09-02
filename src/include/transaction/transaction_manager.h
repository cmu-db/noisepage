#pragma once
#include <map>
#include <utility>
#include "common/shared_latch.h"
#include "common/spin_latch.h"
#include "common/typedefs.h"
#include "storage/data_table.h"
#include "storage/log_manager.h"
#include "storage/record_buffer.h"
#include "storage/undo_record.h"
#include "transaction/transaction_context.h"
#include "transaction/transaction_defs.h"

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
  // TODO(Tianyu): Remove this default argument
  explicit TransactionManager(storage::RecordBufferSegmentPool *const buffer_pool, const bool gc_enabled,
                              storage::LogManager *log_manager)
      : buffer_pool_(buffer_pool), gc_enabled_(gc_enabled), log_manager_(log_manager) {}

  /**
   * Begins a transaction.
   * @return transaction context for the newly begun transaction
   */
  TransactionContext *BeginTransaction();

  /**
   * Commits a transaction, making all of its changes visible to others.
   * @param txn the transaction to commit
   * @param callback callback function that the transaction manager will execute as soon as all log records of the
   *                 given transaction is flushed out. Needless to say, short callbacks that delegates work
   *                 to a different thread is preferable.
   * @return commit timestamp of this transaction
   */
  timestamp_t Commit(TransactionContext *txn, const std::function<void()> &callback);

  /**
   * Aborts a transaction, rolling back its changes (if any).
   * @param txn the transaction to abort.
   */
  void Abort(TransactionContext *txn);

  /**
   * Get the oldest transaction alive in the system at this time. Because of concurrent operations, it
   * is not guaranteed that upon return the txn is still alive. However, it is guaranteed that the return
   * timestamp is older than any transactions live.
   * @return timestamp that is older than any transactions alive
   */
  timestamp_t OldestTransactionStartTime() const;

  /**
   * @return unique timestamp based on current time, and advances one tick
   */
  timestamp_t GetTimestamp() { return time_++; }

  /**
   * @return true if gc_enabled and storing completed txns in local queue, false otherwise
   */
  bool GCEnabled() const { return gc_enabled_; }

  /**
   * Return a copy of the completed txns queue and empty the local version
   * @return copy of the completed txns for the GC to process
   */
  TransactionQueue CompletedTransactionsForGC();

 private:
  storage::RecordBufferSegmentPool *buffer_pool_;
  // TODO(Tianyu): Timestamp generation needs to be more efficient (batches)
  // TODO(Tianyu): We don't handle timestamp wrap-arounds. I doubt this would be an issue though.
  std::atomic<timestamp_t> time_{timestamp_t(0)};

  // TODO(Tianyu): This is the famed HyPer Latch. We will need to re-evaluate performance later.
  common::SharedLatch commit_latch_;

  // TODO(Matt): consider a different data structure if this becomes a measured bottleneck
  mutable common::SpinLatch table_latch_;
  std::map<timestamp_t, TransactionContext *> curr_running_txns_;

  bool gc_enabled_ = false;
  TransactionQueue completed_txns_;
  storage::LogManager *const log_manager_;

  void Rollback(timestamp_t txn_id, const storage::UndoRecord &record) const;
};
}  // namespace terrier::transaction
