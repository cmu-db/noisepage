#pragma once

#include "storage/data_table.h"
#include "storage/projected_row.h"
#include "transaction/timestamp_manager.h"
#include "transaction/transaction_defs.h"

namespace noisepage::storage {
/**
 * Encapsulates information common to all log records in memory (i.e. a header). Note that the disk representation of
 * log records can be different. Depending on the type of this LogRecord the underlying body can be
 * obtained with the @see GetUnderlyingRecordBodyAs method.
 */
class LogRecord {
 public:
  MEM_REINTERPRETATION_ONLY(LogRecord)

  /**
   * @return type of this LogRecord
   */
  LogRecordType RecordType() const { return type_; }
  /**
   * @return size of the whole record (header + body)
   */
  uint32_t Size() const { return size_; }
  /**
   * @return begin timestamp of the transaction that generated this log record
   */
  transaction::timestamp_t TxnBegin() const { return txn_begin_; }

  /**
   * Get the underlying record body as a certain type, determined from RecordType().
   * @tparam UnderlyingType type of the underlying record body, This must be equal to the return
   *                        value of the RecordType() call
   * @return pointer to the underlying record body
   */
  template <class UnderlyingType>
  UnderlyingType *GetUnderlyingRecordBodyAs() {
    NOISEPAGE_ASSERT(UnderlyingType::RecordType() == type_, "Attempting to access incompatible log record types");
    return reinterpret_cast<UnderlyingType *>(varlen_contents_);
  }

  /**
   * Get the underlying record body as a certain type, determined from RecordType().
   * @tparam UnderlyingType type of the underlying record body, This must be equal to the return
   *                        value of the RecordType() call
   * @return const pointer to the underlying record body
   */
  template <class UnderlyingType>
  const UnderlyingType *GetUnderlyingRecordBodyAs() const {
    NOISEPAGE_ASSERT(UnderlyingType::RecordType() == type_, "Attempting to access incompatible log record types");
    return reinterpret_cast<const UnderlyingType *>(varlen_contents_);
  }

  /**
   * Initialize the header of a LogRecord using the given parameters
   * @param head pointer location to initialize, this is also the returned address (reinterpreted)
   * @param type type of the underlying record
   * @param size size of the entire record, in memory, in bytes
   * @param txn_begin begin timestamp of the transaction that generated this log record
   * @return pointer to the start of the initialized record header
   */
  static LogRecord *InitializeHeader(byte *const head, const LogRecordType type, const uint32_t size,
                                     const transaction::timestamp_t txn_begin) {
    auto *result = reinterpret_cast<LogRecord *>(head);
    result->type_ = type;
    result->size_ = size;
    result->txn_begin_ = txn_begin;
    return result;
  }

 private:
  friend class transaction::TransactionContext;
  /* Header common to all log records */
  LogRecordType type_;
  uint32_t size_;
  transaction::timestamp_t txn_begin_;
  // This needs to be aligned to 8 bytes to ensure the real size of RedoRecord (plus actual ProjectedRow) is also
  // a multiple of 8.
  uint64_t varlen_contents_[0];
};

// TODO(Tianyu): I don't think this has any effect on correctness, but for consistency's sake
static_assert(sizeof(LogRecord) % 8 == 0, "a projected row inside the log record needs to be aligned to 8 bytes");

/**
 * Record body of a Redo. The header is stored in the LogRecord class that would presumably return this
 * object.
 */
class RedoRecord {
 public:
  MEM_REINTERPRETATION_ONLY(RedoRecord)

  /**
   * @return the tuple slot changed by this redo record
   */
  TupleSlot GetTupleSlot() const { return tuple_slot_; }

  /**
   * @return database oid for this redo record
   */
  catalog::db_oid_t GetDatabaseOid() const { return db_oid_; }

  /**
   * @return table oid for this redo record
   */
  catalog::table_oid_t GetTableOid() const { return table_oid_; }

  /**
   * @return the tuple slot changed by this redo record
   */
  void SetTupleSlot(const TupleSlot tuple_slot) { tuple_slot_ = tuple_slot; }

  /**
   * @return inlined delta that (was/is to be) applied to the tuple in the table
   */
  ProjectedRow *Delta() { return reinterpret_cast<ProjectedRow *>(varlen_contents_); }

  /**
   * @return const inlined delta that (was/is to be) applied to the tuple in the table
   */
  const ProjectedRow *Delta() const { return reinterpret_cast<const ProjectedRow *>(varlen_contents_); }

  /**
   * @return type of record this type of body holds
   */
  static constexpr LogRecordType RecordType() { return LogRecordType::REDO; }

  /**
   * @return Size of the entire record of this type, in bytes, in memory, if the underlying Delta is to have the same
   * structure as described by the given initializer.
   */
  static uint32_t Size(const ProjectedRowInitializer &initializer) {
    return static_cast<uint32_t>(sizeof(LogRecord) + sizeof(RedoRecord) + initializer.ProjectedRowSize());
  }

  /**
   * Initialize an entire LogRecord (header included) to have an underlying redo record, using the parameters supplied
   * @param head pointer location to initialize, this is also the returned address (reinterpreted)
   * @param txn_begin begin timestamp of the transaction that generated this log record
   * @param db_oid database oid of this redo record
   * @param table_oid table oid of this redo record
   * @param initializer the initializer to use for the underlying
   * @return pointer to the initialized log record, always equal in value to the given head
   */
  static LogRecord *Initialize(byte *const head, const transaction::timestamp_t txn_begin,
                               const catalog::db_oid_t db_oid, const catalog::table_oid_t table_oid,
                               const ProjectedRowInitializer &initializer) {
    LogRecord *result = LogRecord::InitializeHeader(head, LogRecordType::REDO, Size(initializer), txn_begin);
    auto *body = result->GetUnderlyingRecordBodyAs<RedoRecord>();
    body->db_oid_ = db_oid;
    body->table_oid_ = table_oid;
    body->tuple_slot_ = TupleSlot(nullptr, 0);
    initializer.InitializeRow(body->Delta());
    return result;
  }

  /**
   * TODO(Tianyu): Remove this as we clean up serialization
   * Hacky back door for BufferedLogReader. Essentially, the current implementation dumps memory content straight out
   * to disk, which we then read directly back into the projected row. After the serialization format is decided, write
   * a real factory method for recovery.
   * @param head
   * @param size
   * @param txn_begin
   * @param db_oid database oid of this redo record
   * @param table_oid table oid of this redo record
   * @param tuple_slot
   * @return
   */
  static LogRecord *PartialInitialize(byte *const head, const uint32_t size, const transaction::timestamp_t txn_begin,
                                      const catalog::db_oid_t db_oid, const catalog::table_oid_t table_oid,
                                      const TupleSlot tuple_slot) {
    LogRecord *result = LogRecord::InitializeHeader(head, LogRecordType::REDO, size, txn_begin);
    auto *body = result->GetUnderlyingRecordBodyAs<RedoRecord>();
    body->db_oid_ = db_oid;
    body->table_oid_ = table_oid;
    body->tuple_slot_ = tuple_slot;
    return result;
  }

 private:
  // TODO(Tianyu): We will eventually need to consult the DataTable to determine how to serialize a given column
  // (varlen? compressed? from an outdated schema?) For now we just assume we can serialize everything out as-is,
  // and the reader still have access to the layout on recovery and can deserialize. This is why we are not
  // just taking an oid.
  catalog::db_oid_t db_oid_;
  catalog::table_oid_t table_oid_;
  TupleSlot tuple_slot_;
  // This needs to be aligned to 8 bytes to ensure the real size of RedoRecord (plus actual ProjectedRow) is also
  // a multiple of 8.
  uint64_t varlen_contents_[0];
};

// TODO(Tianyu): Same here
static_assert(sizeof(RedoRecord) % 8 == 0, "a projected row inside the redo record needs to be aligned to 8 bytes");

/**
 * Record body of a Delete. The header is stored in the LogRecord class that would presumably return this
 * object.
 */
class DeleteRecord {
 public:
  MEM_REINTERPRETATION_ONLY(DeleteRecord)
  /**
   * @return type of record this type of body holds
   */
  static constexpr LogRecordType RecordType() { return LogRecordType::DELETE; }

  /**
   * @return Size of the entire record of this type, in bytes, in memory.
   */
  static uint32_t Size() { return static_cast<uint32_t>(sizeof(LogRecord) + sizeof(DeleteRecord)); }

  /**
   * Initialize an entire LogRecord (header included) to have an underlying delete record, using the parameters
   * supplied.
   *
   * @param head pointer location to initialize, this is also the returned address (reinterpreted)
   * @param txn_begin begin timestamp of the transaction that generated this log record
   * @param db_oid database oid of this delete record
   * @param table_oid table oid of this delete record
   * @param slot the tuple slot this delete applies to
   * @return pointer to the initialized log record, always equal in value to the given head
   */
  static LogRecord *Initialize(byte *const head, const transaction::timestamp_t txn_begin,
                               const catalog::db_oid_t db_oid, const catalog::table_oid_t table_oid,
                               const TupleSlot slot) {
    auto *result = LogRecord::InitializeHeader(head, LogRecordType::DELETE, Size(), txn_begin);
    auto *body = result->GetUnderlyingRecordBodyAs<DeleteRecord>();
    body->db_oid_ = db_oid;
    body->table_oid_ = table_oid;
    body->tuple_slot_ = slot;
    return result;
  }

  /**
   * @return the tuple slot changed by this delete record
   */
  TupleSlot GetTupleSlot() const { return tuple_slot_; }

  /**
   * @return database oid for this delete record
   */
  catalog::db_oid_t GetDatabaseOid() const { return db_oid_; }

  /**
   * @return table oid for this delete record
   */
  catalog::table_oid_t GetTableOid() const { return table_oid_; }

 private:
  catalog::db_oid_t db_oid_;
  catalog::table_oid_t table_oid_;
  TupleSlot tuple_slot_;
};

/**
 * Record body of a Commit. The header is stored in the LogRecord class that would presumably return this
 * object.
 */
class CommitRecord {
 public:
  MEM_REINTERPRETATION_ONLY(CommitRecord)

  /**
   * @return type of record this type of body holds
   */
  static constexpr LogRecordType RecordType() { return LogRecordType::COMMIT; }

  /**
   * @return Size of the entire record of this type, in bytes, in memory.
   */
  static uint32_t Size() { return static_cast<uint32_t>(sizeof(LogRecord) + sizeof(CommitRecord)); }

  /**
   * Initialize an entire LogRecord (header included) to have an underlying commit record, using the parameters
   * supplied.
   *
   * @param head pointer location to initialize, this is also the returned address (reinterpreted)
   * @param txn_begin begin timestamp of the transaction that generated this log record
   * @param txn_commit the commit timestamp of the transaction that generated this log record
   * @param commit_callback function pointer of the callback to invoke when commit is
   * @param commit_callback_arg a void * argument that can be passed to the callback function when invoked
   * @param oldest_active_txn start timestamp of the oldest active txn at the time of this commit
   * @param is_read_only indicates whether the transaction generating this log record is read-only or not
   * @param txn pointer to the committing transaction
   * @param timestamp_manager pointer to timestamp manager who provided timestamp to txn. Used to notify of
   * serialization
   * @return pointer to the initialized log record, always equal in value to the given head
   */
  // TODO(Tianyu): txn should contain a lot of the information here. Maybe we can simplify the function.
  // Note that however when reading log records back in we will not have a proper transaction.
  static LogRecord *Initialize(byte *const head, const transaction::timestamp_t txn_begin,
                               const transaction::timestamp_t txn_commit, transaction::callback_fn commit_callback,
                               void *commit_callback_arg, const transaction::timestamp_t oldest_active_txn,
                               const bool is_read_only, transaction::TransactionContext *const txn,
                               transaction::TimestampManager *const timestamp_manager) {
    auto *result = LogRecord::InitializeHeader(head, LogRecordType::COMMIT, Size(), txn_begin);
    auto *body = result->GetUnderlyingRecordBodyAs<CommitRecord>();
    body->txn_commit_ = txn_commit;
    body->commit_callback_ = commit_callback;
    body->commit_callback_arg_ = commit_callback_arg;
    body->oldest_active_txn_ = oldest_active_txn;
    body->timestamp_manager_ = timestamp_manager;
    body->txn_ = txn;
    body->is_read_only_ = is_read_only;
    return result;
  }

  /**
   * @return the commit time of the transaction that generated this log record
   */
  transaction::timestamp_t CommitTime() const { return txn_commit_; }

  /**
   * @return the start time of the oldest active transaction at the time that this txn committed
   */
  transaction::timestamp_t OldestActiveTxn() const { return oldest_active_txn_; }

  /**
   * @return function pointer of the transaction commit callback. Not necessarily populated if read back in from disk.
   */
  transaction::callback_fn CommitCallback() const { return commit_callback_; }

  /**
   * @return pointer to timestamp manager who manages committing txn
   */
  transaction::TimestampManager *TimestampManager() const { return timestamp_manager_; }

  /**
   * @return pointer to the committing transaction.
   */
  transaction::TransactionContext *Txn() const { return txn_; }

  /**
   * @return argument to the transaction callback
   */
  void *CommitCallbackArg() const { return commit_callback_arg_; }

  /**
   * @return true if and only if the transaction generating this commit record was read-only
   */
  bool IsReadOnly() const { return is_read_only_; }

 private:
  transaction::timestamp_t txn_commit_;
  transaction::callback_fn commit_callback_;
  void *commit_callback_arg_;
  transaction::timestamp_t oldest_active_txn_;
  // TODO(TIanyu): Can replace the other arguments
  // More specifically, commit timestamp and read_only can be inferred from looking inside the transaction context
  transaction::TransactionContext *txn_;
  transaction::TimestampManager *timestamp_manager_;
  bool is_read_only_;
};

/**
 * Record body of an Abort. The header is stored in the LogRecord class that would presumably return this
 * object. An AbortRecord is only generated if an aborted transaction previously handed off a log buffer to the
 * log manager
 */
class AbortRecord {
 public:
  MEM_REINTERPRETATION_ONLY(AbortRecord)

  /**
   * @return type of record this type of body holds
   */
  static constexpr LogRecordType RecordType() { return LogRecordType::ABORT; }

  /**
   * @return Size of the entire record of this type, in bytes, in memory.
   */
  static uint32_t Size() { return static_cast<uint32_t>(sizeof(LogRecord) + sizeof(AbortRecord)); }

  /**
   * Initialize an entire LogRecord (header included) to have an underlying abort record, using the parameters
   * supplied.
   *
   * @param head pointer location to initialize, this is also the returned address (reinterpreted)
   * @param txn_begin begin timestamp of the transaction that generated this log record
   * @param txn transaction that is aborted
   * @param timestamp_manager pointer to timestamp manager who provided timestamp to txn. Used to notify of
   * serialization
   * @return pointer to the initialized log record, always equal in value to the given head
   */
  static LogRecord *Initialize(byte *const head, const transaction::timestamp_t txn_begin,
                               transaction::TransactionContext *txn,
                               transaction::TimestampManager *const timestamp_manager) {
    auto *result = LogRecord::InitializeHeader(head, LogRecordType::ABORT, Size(), txn_begin);
    auto *body = result->GetUnderlyingRecordBodyAs<AbortRecord>();
    body->timestamp_manager_ = timestamp_manager;
    body->txn_ = txn;
    return result;
  }

  /**
   * @return pointer to the aborting transaction.
   */
  transaction::TransactionContext *Txn() const { return txn_; }

  /**
   * @return pointer to timestamp manager who manages aborting txn
   */
  transaction::TimestampManager *TimestampManager() const { return timestamp_manager_; }

 private:
  transaction::TimestampManager *timestamp_manager_;
  transaction::TransactionContext *txn_;
};
}  // namespace noisepage::storage
