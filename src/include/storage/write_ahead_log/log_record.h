#pragma once
#include "storage/data_table.h"
#include "storage/projected_row.h"

namespace terrier::storage {
/**
 * Types of LogRecords
 */
enum class LogRecordType : uint8_t { REDO = 1, COMMIT };

/**
 * Encapsulates information common to all log records in memory (i.e. a header). Note that the disk representation of
 * log records can be different. Depending on the type of this LogRecord the underlying body can be
 * obtained with the @see GetUnderlyingRecordBodyAs method.
 */
class LogRecord {
 public:
  MEM_REINTERPRETAION_ONLY(LogRecord)

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
  timestamp_t TxnBegin() const { return txn_begin_; }

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
  static LogRecord *InitializeHeader(void *head, LogRecordType type, uint32_t size, timestamp_t txn_begin) {
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
  timestamp_t txn_begin_;
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
  MEM_REINTERPRETAION_ONLY(RedoRecord)

  /**
   * @return pointer to the DataTable that this Redo is concerned with
   */
  DataTable *GetDataTable() const {
    uintptr_t ptr_value = *reinterpret_cast<const uintptr_t *>(varlen_contents_);
    return reinterpret_cast<DataTable *>(ptr_value);
  }

  /**
   * @return the tuple slot changed by this redo record
   */
  TupleSlot GetTupleSlot() const { return tuple_slot_; }

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
  static LogRecord *Initialize(void *head, timestamp_t txn_begin, DataTable *table, TupleSlot tuple_slot,
                               const ProjectedRowInitializer &initializer) {
    LogRecord *result = LogRecord::InitializeHeader(head, LogRecordType::REDO, Size(initializer), txn_begin);
    auto *body = result->GetUnderlyingRecordBodyAs<RedoRecord>();
    body->table_ = table;
    body->tuple_slot_ = tuple_slot;
    initializer.InitializeRow(body->Delta());
    return result;
  }

 private:
  // TODO(Tianyu): We will eventually need to consult the DataTable to determine how to serialize a given column
  // (varlen? compressed? from an outdated schema?) For now we just assume we can serialize everything out as-is,
  // and the reader still have access to the layout on recovery and can deserialize. This is why we are not
  // just taking an oid.
  DataTable *table_;
  TupleSlot tuple_slot_;
  // This needs to be aligned to 8 bytes to ensure the real size of RedoRecord (plus actual ProjectedRow) is also
  // a multiple of 8.
  uint64_t varlen_contents_[0];
};

// TODO(Tianyu): Same here
static_assert(sizeof(RedoRecord) % 8 == 0, "a projected row inside the redo record needs to be aligned to 8 bytes");

/**
 * Record body of a Commit. The header is stored in the LogRecord class that would presumably return this
 * object.
 */
class CommitRecord {
 public:
  MEM_REINTERPRETAION_ONLY(CommitRecord)

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
   * @return pointer to the initialized log record, always equal in value to the given head
   */
  static LogRecord *Initialize(void *head, timestamp_t txn_begin, timestamp_t txn_commit) {
    auto *result = LogRecord::InitializeHeader(head, LogRecordType::COMMIT, Size(), txn_begin);
    auto *body = result->GetUnderlyingRecordBodyAs<CommitRecord>();
    body->txn_commit_ = txn_commit;
    return result;
  }

  /**
   * @return the commit time of the transaction that generated this log record
   */
  timestamp_t CommitTime() const { return txn_commit_; }

 private:
  timestamp_t txn_commit_;
};
}  // namespace terrier::storage
