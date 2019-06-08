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
#include "storage/version_chain_gc.h"
#include "storage/write_ahead_log/log_manager.h"
#include "transaction/transaction_context.h"
#include "transaction/transaction_defs.h"
#include "transaction/timestamp_manager.h"
#include "transaction/deferred_action_manager.h"

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
  TransactionManager(TimestampManager *timestamp_manager,
                     storage::RecordBufferSegmentPool *const buffer_pool,
                     DeferredActionManager *deferred_action_manager,
                     storage::VersionChainGC *version_chain_gc,
                     storage::LogManager *log_manager)
      : timestamp_manager_(timestamp_manager),
        buffer_pool_(buffer_pool),
        deferred_action_manager_(deferred_action_manager),
        version_chain_gc_(version_chain_gc),
        log_manager_(log_manager) {
    TERRIER_ASSERT(timestamp_manager_ != DISABLED, "TransactionManager cannot function without a timestamp manager");
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
  // TODO(Tianyu): In case anyone wonders, we are taking a C-style function pointer instead of more modern lambdas
  // because we embed the callback information into commit records, and lambdas are not PODs we can initialize
  // from any junk memory
  timestamp_t Commit(TransactionContext *txn, transaction::callback_fn callback, void *callback_arg);

  /**
   * Aborts a transaction, rolling back its changes (if any).
   * @param txn the transaction to abort.
   * @return abort timestamp of this transaction.
   */
  timestamp_t Abort(TransactionContext *txn);

 private:
  TimestampManager *timestamp_manager_;
  storage::RecordBufferSegmentPool *buffer_pool_;
  DeferredActionManager *deferred_action_manager_;
  storage::VersionChainGC *version_chain_gc_;
  storage::LogManager *const log_manager_;
  common::Gate txn_gate_;

  timestamp_t ReadOnlyCommitCriticalSection(TransactionContext *txn, transaction::callback_fn callback,
                                            void *callback_arg);

  timestamp_t UpdatingCommitCriticalSection(TransactionContext *txn, transaction::callback_fn callback,
                                            void *callback_arg);

  void LogCommit(TransactionContext *txn, timestamp_t commit_time, transaction::callback_fn callback,
                 void *callback_arg);

  static void Rollback(TransactionContext *txn, const storage::UndoRecord &record);

  static void DeallocateColumnUpdateIfVarlen(TransactionContext *txn, storage::UndoRecord *undo,
                                      uint16_t projection_list_index,
                                      const storage::TupleAccessStrategy &accessor);

  static void DeallocateInsertedTupleIfVarlen(TransactionContext *txn, storage::UndoRecord *undo,
                                       const storage::TupleAccessStrategy &accessor);

  static void RecaimLastUpdateOnAbort(TransactionContext *txn);
};
}  // namespace terrier::transaction
