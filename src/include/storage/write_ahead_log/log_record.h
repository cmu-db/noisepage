#pragma once
#include "storage/data_table.h"
#include "storage/projected_row.h"
#include "transaction/transaction_defs.h"

namespace terrier::storage {
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
    TERRIER_ASSERT(UnderlyingType::RecordType() == type_, "Attempting to access incompatible log record types");
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
    TERRIER_ASSERT(UnderlyingType::RecordType() == type_, "Attempting to access incompatible log record types");
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
   * @return pointer to the DataTable that this Redo is concerned with
   */
  DataTable *GetDataTable() const { return table_; }

  /**
   * @return the tuple slot changed by this redo record
   */
  TupleSlot GetTupleSlot() const { return tuple_slot_; }

  // TODO(Tianyu): Potentially need a setter for Inserts, because we know the TupleSlot after insert

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
   * @param table the DataTable that this Redo is concerned with
   * @param tuple_slot the tuple slot changed by this redo record
   * @param initializer the initializer to use for the underlying
   * @return pointer to the initialized log record, always equal in value to the given head
   */
  static LogRecord *Initialize(byte *const head, const transaction::timestamp_t txn_begin, DataTable *const table,
                               const TupleSlot tuple_slot, const ProjectedRowInitializer &initializer) {
    LogRecord *result = LogRecord::InitializeHeader(head, LogRecordType::REDO, Size(initializer), txn_begin);
    auto *body = result->GetUnderlyingRecordBodyAs<RedoRecord>();
    body->table_ = table;
    body->tuple_slot_ = tuple_slot;
    initializer.InitializeRow(body->Delta());
    return result;
  }

 private:
  // TODO(Tianyu): We will eventually need to consult the DataTable to determine how to serialize a given column
  //  (varlen? compressed? from an outdated schema?) For now we just assume we can serialize everything out as-is,
  //  and the reader still have access to the layout on recovery and can deserialize. This is why we are not
  //  just taking an oid.
  DataTable *table_;
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
   * @param table the data table this delete points to
   * @param slot the tuple slot this delete applies to
   * @return pointer to the initialized log record, always equal in value to the given head
   */
  static LogRecord *Initialize(byte *const head, const transaction::timestamp_t txn_begin, DataTable *const table,
                               TupleSlot slot) {
    auto *result = LogRecord::InitializeHeader(head, LogRecordType::DELETE, Size(), txn_begin);
    auto *body = result->GetUnderlyingRecordBodyAs<DeleteRecord>();
    body->table_ = table;
    body->tuple_slot_ = slot;
    return result;
  }

  /**
   * @return pointer to the DataTable that this delete is concerned with
   */
  DataTable *GetDataTable() const { return table_; }

  /**
   * @return the tuple slot changed by this delete record
   */
  TupleSlot GetTupleSlot() const { return tuple_slot_; }

 private:
  // TODO(Tianyu): Change to oid maybe?
  DataTable *table_;
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
   * @param callback function pointer of the callback to invoke when commit is
   * @param callback_arg a void * argument that can be passed to the callback function when invoked
   * @param is_read_only indicates whether the transaction generating this log record is read-only or not
   * @param txn pointer to the committing transaction
   * @return pointer to the initialized log record, always equal in value to the given head
   */
  // TODO(Tianyu): txn should contain a lot of the information here. Maybe we can simplify the function.
  // Note that however when reading log records back in we will not have a proper transaction.
  static LogRecord *Initialize(byte *const head, const transaction::timestamp_t txn_begin,
                               const transaction::timestamp_t txn_commit, transaction::callback_fn callback,
                               void *callback_arg, bool is_read_only, transaction::TransactionContext *txn) {
    auto *result = LogRecord::InitializeHeader(head, LogRecordType::COMMIT, Size(), txn_begin);
    auto *body = result->GetUnderlyingRecordBodyAs<CommitRecord>();
    body->txn_commit_ = txn_commit;
    body->callback_ = callback;
    body->callback_arg_ = callback_arg;
    body->txn_ = txn;
    body->is_read_only_ = is_read_only;
    return result;
  }

  /**
   * @return the commit time of the transaction that generated this log record
   */
  transaction::timestamp_t CommitTime() const { return txn_commit_; }

  /**
   * @return function pointer of the transaction callback. Not necessarily populated if read back in from disk.
   */
  transaction::callback_fn Callback() const { return callback_; }

  /**
   * @return pointer to the committing transaction.
   */
  transaction::TransactionContext *Txn() const { return txn_; }

  /**
   * @return argument to the transaction callback
   */
  void *CallbackArg() const { return callback_arg_; }

  /**
   * @return true if and only if the transaction generating this commit record was read-only
   */
  bool IsReadOnly() const { return is_read_only_; }

 private:
  transaction::timestamp_t txn_commit_;
  transaction::callback_fn callback_;
  void *callback_arg_;
  // TODO(TIanyu): Can replace the other arguments
  // More specifically, commit timestamp and read_only can be inferred from looking inside the transaction context
  transaction::TransactionContext *txn_;
  bool is_read_only_;
};
}  // namespace terrier::storage
