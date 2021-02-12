#pragma once

#include <queue>
#include <unordered_set>
#include <utility>

#include "common/gate.h"
#include "common/spin_latch.h"
#include "common/strong_typedef.h"
#include "storage/record_buffer.h"
#include "storage/undo_record.h"
#include "transaction/timestamp_manager.h"
#include "transaction/transaction_context.h"
#include "transaction/transaction_defs.h"

namespace noisepage::storage {
class LogManager;
}  // namespace noisepage::storage

namespace noisepage::transaction {
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
   * @param timestamp_manager timestamp manager that manages timestamps for transactions
   * @param deferred_action_manager deferred action manager to use for transactions
   * @param buffer_pool the buffer pool to use for transaction undo buffers
   * @param gc_enabled true if txns should be stored in a local queue to hand off to the GC, false otherwise
   * @param wal_async_commit_enable true if commit callbacks should be invoked by TransactionManager at commit time
   * rather than waiting until durable on disk and being invoked by the WAL worker. Doesn't make sense to set to true if
   * WAL is not enabled.
   * @param log_manager the log manager in the system, or DISABLED(nulllptr) if logging is turned off.
   */
  TransactionManager(const common::ManagedPointer<TimestampManager> timestamp_manager,
                     const common::ManagedPointer<DeferredActionManager> deferred_action_manager,
                     const common::ManagedPointer<storage::RecordBufferSegmentPool> buffer_pool, const bool gc_enabled,
                     const bool wal_async_commit_enable, const common::ManagedPointer<storage::LogManager> log_manager)
      : timestamp_manager_(timestamp_manager),
        deferred_action_manager_(deferred_action_manager),
        buffer_pool_(buffer_pool),
        gc_enabled_(gc_enabled),
        wal_async_commit_enable_(wal_async_commit_enable),
        log_manager_(log_manager) {
    NOISEPAGE_ASSERT(timestamp_manager_ != DISABLED, "transaction manager cannot function without a timestamp manager");
    NOISEPAGE_ASSERT(!wal_async_commit_enable_ || (wal_async_commit_enable_ && log_manager_ != DISABLED),
                     "Doesn't make sense to enable async commit without enabling logging.");
  }

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
   * @return abort timestamp of this transaction.
   */
  timestamp_t Abort(TransactionContext *txn);

  /**
   * @return true if gc_enabled and storing completed txns in local queue, false otherwise
   */
  bool GCEnabled() const { return gc_enabled_; }

  /**
   * TODO(Ling): hack for fake gc; remove it in the future
   * @return number of transactions unlinked in this gc period
   */
  uint32_t NumUnlinked() { return num_unlinked_.exchange(0); }

  /**
   * TODO(Ling): hack for fake gc; remove it in the future
   * @return number of transactions deallocated in this gc period
   */
  uint32_t NumDeallocated() { return num_deallocated_.exchange(0); }

  /**
   * Note: for log tests, as it will compare the redo records,
   *    we don't want to gc the transactions before comparing the result with the desired values;
   *    For GC tests and DAF tests, are we are testing on the result of each invocation of Process() function,
   *    we need a way to stop the automatic invocation.
   * Set if the transaction manager cooperatively clean up the deferred action queue
   * @param value True if use cooperative gc at end of transaction
   */
  void SetCooperativeGC(bool value) { cooperative_gc_ = value; }

 private:
  const common::ManagedPointer<TimestampManager> timestamp_manager_;
  const common::ManagedPointer<DeferredActionManager> deferred_action_manager_;
  const common::ManagedPointer<storage::RecordBufferSegmentPool> buffer_pool_;
  const bool gc_enabled_ = false;
  const bool wal_async_commit_enable_ = false;

  common::Gate txn_gate_;

  TransactionQueue completed_txns_;
  const common::ManagedPointer<storage::LogManager> log_manager_;

  // TODO(Ling): two counters to fake the original gc behavior.
  //  eventually we will removed them after completely integrate the deferred action framework
  std::atomic<int> num_unlinked_{0};
  std::atomic<int> num_deallocated_{0};

  // This variable is used to set if the transaction manager will do cooperative cleaning of the deferred action queue
  bool cooperative_gc_{true};

  timestamp_t UpdatingCommitCriticalSection(TransactionContext *txn);

  void LogCommit(TransactionContext *txn, timestamp_t commit_time, transaction::callback_fn commit_callback,
                 void *commit_callback_arg, timestamp_t oldest_active_txn);

  void LogAbort(TransactionContext *txn);

  void Rollback(TransactionContext *txn, const storage::UndoRecord &record) const;

  void DeallocateColumnUpdateIfVarlen(TransactionContext *txn, storage::UndoRecord *undo,
                                      uint16_t projection_list_index,
                                      const storage::TupleAccessStrategy &accessor) const;

  void DeallocateInsertedTupleIfVarlen(TransactionContext *txn, storage::UndoRecord *undo,
                                       const storage::TupleAccessStrategy &accessor) const;
  void GCLastUpdateOnAbort(TransactionContext *txn);

  void CleanTransaction(TransactionContext *txn);
};
}  // namespace noisepage::transaction
