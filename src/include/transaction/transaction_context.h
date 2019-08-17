#pragma once
#include <vector>
#include "common/object_pool.h"
#include "common/strong_typedef.h"
#include "storage/data_table.h"
#include "storage/record_buffer.h"
#include "storage/storage_defs.h"
#include "storage/tuple_access_strategy.h"
#include "storage/undo_record.h"
#include "storage/write_ahead_log/log_record.h"
#include "transaction/transaction_util.h"

namespace terrier::storage {
class GarbageCollector;
class LogManager;
class BlockCompactor;
class LogSerializerTask;
class SqlTable;
class WriteAheadLoggingTests;
}  // namespace terrier::storage

namespace terrier::transaction {
/**
 * A transaction context encapsulates the information kept while the transaction is running
 */
class TransactionContext {
 public:
  /**
   * Constructs a new transaction context.
   *
   * @warning In the src/ folder this should only be called in TransactionManager::BeginTransaction to adhere to MVCC
   * semantics. Tests are allowed to deterministically construct them in ways that violate the current MVCC semantics.
   * @warning Beware that the buffer pool given must be the same one the log manager uses,
   * if logging is enabled.
   * @param start the start timestamp of the transaction. Should be unique within the system.
   * @param finish in HyPer parlance this is txn id. Should be larger than all start times and commit times in current
   * MVCC semantics
   * @param buffer_pool the buffer pool to draw this transaction's undo buffer from
   * @param log_manager pointer to log manager in the system, or nullptr, if logging is disabled
   */
  TransactionContext(const timestamp_t start, const timestamp_t finish,
                     storage::RecordBufferSegmentPool *const buffer_pool, storage::LogManager *const log_manager)
      : start_time_(start), finish_time_(finish), undo_buffer_(buffer_pool), redo_buffer_(log_manager, buffer_pool) {}

  /**
   * @warning In the src/ folder this should only be called by the Garbage Collector to adhere to MVCC semantics. Tests
   * are allowed to deterministically delete them in ways that violate the current MVCC semantics, but you should really
   * know what you're doing when you delete a TransactionContext since its UndoRecords may still be pointed to by a
   * DataTable.
   */
  ~TransactionContext() {
    for (const byte *ptr : loose_ptrs_) delete[] ptr;
  }

  /**
   * @warning Unless you are the garbage collector, this method is unlikely to be of use.
   * @return whether this transaction has been aborted. Note that this is different from being "uncommitted". Some one
   *         needs to have called Abort() explicitly on this transaction for this function to return true.
   */
  bool Aborted() const { return aborted_; }

  /**
   * @return start time of this transaction. Can be used as a unique identifier of this object in the current MVCC
   * semantics because it is both constant and unique within the system
   */
  timestamp_t StartTime() const { return start_time_; }

  /**
   * @return finish time of this transaction if it has been aborted or logged as a commit. Otherwise, current
   * MVCC semantics define it as StartTime + INT64_MIN. TransactionContexts generated outside of the TransactionManager
   * (i.e. in tests) may not reflect this. Should NOT be used as a unique identifier of this object because its value
   * changes at txn completion in the current MVCC semantics.
   */
  timestamp_t FinishTime() const { return finish_time_.load(); }

  /**
   * Reserve space on this transaction's undo buffer for a record to log the update given
   * @param table pointer to the updated DataTable object
   * @param slot the TupleSlot being updated
   * @param redo the content of the update
   * @return a persistent pointer to the head of a memory chunk large enough to hold the undo record
   */
  storage::UndoRecord *UndoRecordForUpdate(storage::DataTable *const table, const storage::TupleSlot slot,
                                           const storage::ProjectedRow &redo) {
    const uint32_t size = storage::UndoRecord::Size(redo);
    return storage::UndoRecord::InitializeUpdate(undo_buffer_.NewEntry(size), finish_time_.load(), slot, table, redo);
  }

  /**
   * Reserve space on this transaction's undo buffer for a record to log the insert given
   * @param table pointer to the updated DataTable object
   * @param slot the TupleSlot inserted
   * @return a persistent pointer to the head of a memory chunk large enough to hold the undo record
   */
  storage::UndoRecord *UndoRecordForInsert(storage::DataTable *const table, const storage::TupleSlot slot) {
    byte *const result = undo_buffer_.NewEntry(sizeof(storage::UndoRecord));
    return storage::UndoRecord::InitializeInsert(result, finish_time_.load(), slot, table);
  }

  /**
   * Reserve space on this transaction's undo buffer for a record to log the delete given
   * @param table pointer to the updated DataTable object
   * @param slot the TupleSlot being deleted
   * @return a persistent pointer to the head of a memory chunk large enough to hold the undo record
   */
  storage::UndoRecord *UndoRecordForDelete(storage::DataTable *const table, const storage::TupleSlot slot) {
    byte *const result = undo_buffer_.NewEntry(sizeof(storage::UndoRecord));
    return storage::UndoRecord::InitializeDelete(result, finish_time_.load(), slot, table);
  }

  /**
   * Expose a record that can hold a change, described by the initializer given, that will be logged out to disk.
   * The change must be written in this space and then used to change the SqlTable.
   * @param db_oid the database oid that this record changes
   * @param table_oid the table oid that this record changes
   * @param initializer the initializer to use for the underlying record
   * @return pointer to the initialized redo record.
   * @warning RedoRecords returned by StageWrite are not guaranteed to remain valid forever. If you call StageWrite
   * again, the previous RedoRecord's buffer may be swapped out, written to disk, and handed back out to another
   * transaction.
   * @warning If you call StageWrite, the operation WILL be logged to disk. If you StageWrite anything that you didn't
   * succeed in writing into the table or decide you don't want to use, the transaction MUST abort.
   */
  storage::RedoRecord *StageWrite(const catalog::db_oid_t db_oid, const catalog::table_oid_t table_oid,
                                  const storage::ProjectedRowInitializer &initializer) {
    const uint32_t size = storage::RedoRecord::Size(initializer);
    auto *const log_record =
        storage::RedoRecord::Initialize(redo_buffer_.NewEntry(size), start_time_, db_oid, table_oid, initializer);
    return log_record->GetUnderlyingRecordBodyAs<storage::RedoRecord>();
  }

  /**
   * Initialize a record that logs a delete, that will be logged out to disk
   * @param db_oid the database oid that this record changes
   * @param table_oid the table oid that this record changes
   * @param slot the slot that this record changes
   * @warning If you call StageDelete, the operation WILL be logged to disk. If you StageDelete anything that you didn't
   * succeed in writing into the table or decide you don't want to use, the transaction MUST abort.
   */
  void StageDelete(const catalog::db_oid_t db_oid, const catalog::table_oid_t table_oid,
                   const storage::TupleSlot slot) {
    const uint32_t size = storage::DeleteRecord::Size();
    storage::DeleteRecord::Initialize(redo_buffer_.NewEntry(size), start_time_, db_oid, table_oid, slot);
  }

  // TODO(Tianyu): We need to discuss what happens to the loose_ptrs field now that we have deferred actions.
  /**
   * @return whether the transaction is read-only
   */
  bool IsReadOnly() const { return undo_buffer_.Empty() && loose_ptrs_.empty(); }

  /**
   * Defers an action to be called if and only if the transaction aborts.  Actions executed LIFO.
   * @param a the action to be executed. A handle to the system's deferred action manager is supplied
   * to enable further deferral of actions
   */
  void RegisterAbortAction(const TransactionEndAction &a) { abort_actions_.push_front(a); }

  /**
   * Defers an action to be called if and only if the transaction aborts.  Actions executed LIFO.
   * @param a the action to be executed
   */
  void RegisterAbortAction(const std::function<void()> &a) {
    RegisterAbortAction([=](transaction::DeferredActionManager * /*unused*/) { a(); });
  }

  /**
   * Defers an action to be called if and only if the transaction commits.  Actions executed LIFO.
   * @warning these actions are run after commit and are not atomic with the commit itself
   * @param a the action to be executed. A handle to the system's deferred action manager is supplied
   * to enable further deferral of actions
   */
  void RegisterCommitAction(const TransactionEndAction &a) { commit_actions_.push_front(a); }

  /**
   * Defers an action to be called if and only if the transaction commits.  Actions executed LIFO.
   * @warning these actions are run after commit and are not atomic with the commit itself
   * @param a the action to be executed.
   */
  void RegisterCommitAction(const std::function<void()> &a) {
    RegisterCommitAction([=](transaction::DeferredActionManager * /*unused*/) { a(); });
  }

  /**
   * Flips the TransactionContext's internal flag that it cannot commit to true. This is checked by the
   * TransactionManager.
   */
  void MustAbort() { must_abort_ = true; }

 private:
  friend class storage::GarbageCollector;
  friend class TransactionManager;
  friend class storage::BlockCompactor;
  friend class storage::LogSerializerTask;
  friend class storage::SqlTable;
  friend class storage::WriteAheadLoggingTests;  // Needs access to redo buffer
  const timestamp_t start_time_;
  std::atomic<timestamp_t> finish_time_;
  storage::UndoBuffer undo_buffer_;
  storage::RedoBuffer redo_buffer_;
  // TODO(Tianyu): Maybe not so much of a good idea to do this. Make explicit queue in GC?
  //
  std::vector<const byte *> loose_ptrs_;

  // These actions will be triggered (not deferred) at abort/commit.
  std::forward_list<TransactionEndAction> abort_actions_;
  std::forward_list<TransactionEndAction> commit_actions_;

  // log manager will set this to be true when log records are processed (not necessarily flushed, but will not be read
  // again in the future), so it can be garbage-collected safely.
  bool log_processed_ = false;
  // We need to know if the transaction is aborted. Even aborted transactions need an "abort" timestamp in order to
  // eliminate the a-b-a race described in DataTable::Select.
  bool aborted_ = false;

  // This flag is used to denote that a physical change to the storage layer (tables or indexes) has occurred that
  // cannot be allowed to commit. Currently, it is flipped by indexes (on unique-key conflicts) or SqlTable (write-write
  // conflicts) and checked in Commit().
  bool must_abort_ = false;
};
}  // namespace terrier::transaction
