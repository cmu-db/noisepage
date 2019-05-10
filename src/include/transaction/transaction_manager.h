#pragma once
#include <queue>
#include <unordered_set>
#include <utility>
#include "common/gate.h"
#include "common/spin_latch.h"
#include "common/strong_typedef.h"
#include "storage/data_table.h"
#include "storage/record_buffer.h"
#include "storage/undo_record.h"
#include "storage/write_ahead_log/log_manager.h"
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
   * @param log_manager the log manager in the system, or LOGGING_DISABLED(nulllptr) if logging is turned off.
   */
  TransactionManager(storage::RecordBufferSegmentPool *const buffer_pool, const bool gc_enabled,
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
   * @param callback function pointer of the callback to invoke when commit is
   * @param callback_arg a void * argument that can be passed to the callback function when invoked
   * @return commit timestamp of this transaction
   */
  timestamp_t Commit(TransactionContext *txn, transaction::callback_fn callback, void *callback_arg);

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

  /**
   * Adds the action to a buffered list of deferred actions.  This action will
   * be triggered no sooner than when the epoch (timestamp of oldest running
   * transaction) is more recent than the time this function was called.
   * @param a functional implementation of the action that is deferred
   */
  void DeferAction(Action a);

  /**
   * Transfers the buffered list of deferred actions to the GC for eventual
   * execution.
   * @return the deferred actions as a sorted queue of pairs where the timestamp is
   *         earliest epoch the associated action can safely fire
   */
  std::queue<std::pair<timestamp_t, Action>> DeferredActionsForGC();

 private:
  storage::RecordBufferSegmentPool *buffer_pool_;
  // TODO(Tianyu): Timestamp generation needs to be more efficient (batches)
  // TODO(Tianyu): We don't handle timestamp wrap-arounds. I doubt this would be an issue though.
  std::atomic<timestamp_t> time_{timestamp_t(0)};

  common::Gate txn_gate_;

  // TODO(Matt): consider a different data structure if this becomes a measured bottleneck
  std::unordered_set<timestamp_t> curr_running_txns_;
  mutable common::SpinLatch curr_running_txns_latch_;

  bool gc_enabled_ = false;
  TransactionQueue completed_txns_;
  storage::LogManager *const log_manager_;

  std::queue<std::pair<timestamp_t, Action>> deferred_actions_;
  mutable common::SpinLatch deferred_actions_latch_;

  timestamp_t ReadOnlyCommitCriticalSection(TransactionContext *txn, transaction::callback_fn callback,
                                            void *callback_arg);

  timestamp_t UpdatingCommitCriticalSection(TransactionContext *txn, transaction::callback_fn callback,
                                            void *callback_arg);

  void LogCommit(TransactionContext *txn, timestamp_t commit_time, transaction::callback_fn callback,
                 void *callback_arg);

  void Rollback(TransactionContext *txn, const storage::UndoRecord &record) const;

  void DeallocateColumnUpdateIfVarlen(TransactionContext *txn, storage::UndoRecord *undo,
                                      uint16_t projection_list_index,
                                      const storage::TupleAccessStrategy &accessor) const;

  void DeallocateInsertedTupleIfVarlen(TransactionContext *txn, storage::UndoRecord *undo,
                                       const storage::TupleAccessStrategy &accessor) const;
  void GCLastUpdateOnAbort(TransactionContext *txn);
};
}  // namespace terrier::transaction
