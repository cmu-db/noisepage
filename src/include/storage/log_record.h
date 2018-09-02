#pragma once
#include "storage/projected_row.h"
namespace terrier::execution {
class SqlTable;
}  // namespace terrier::execution

namespace terrier::storage {
class LogManager;
class RecoveredLog;

// NOLINTNEXTLINE
BETTER_ENUM(LogRecordType, uint8_t, REDO = 1, COMMIT)

class LogRecord {
 public:
  HEAP_ONLY(LogRecord)

  LogRecordType RecordType() const { return type_; }
  uint32_t Size() const { return size_; }
  timestamp_t TxnBegin() const { return txn_begin_; }

  template<class UnderlyingType>
  UnderlyingType *GetUnderlyingRecordBodyAs() {
    return reinterpret_cast<UnderlyingType *>(varlen_contents_);
  }

  static LogRecord *InitializeHeader(void *head, LogRecordType type, uint32_t size, timestamp_t txn_begin) {
    auto *result = reinterpret_cast<LogRecord *>(head);
    result->type_ = type;
    result->size_ = size;
    result->txn_begin_ = txn_begin;
    return result;
  }
 protected:
  /* Header common to all log records */
  LogRecordType type_;
  uint32_t size_;
  timestamp_t txn_begin_;
  // This needs to be aligned to 8 bytes to ensure the real size of RedoRecord (plus actual ProjectedRow) is also
  // a multiple of 8.
  uint64_t varlen_contents_[0];
};

// TODO(Tianyu): I don't think this has any effect on correctness, but for consistency's sake
static_assert(sizeof(LogRecord) % 8 == 0,
              "a projected row inside the log record needs to be aligned to 8 bytes");

class RedoRecordBody {
 public:
  HEAP_ONLY(RedoRecordBody)

  execution::SqlTable *SqlTable() const {
    uintptr_t ptr_value = *reinterpret_cast<const uintptr_t *>(varlen_contents_);
    return reinterpret_cast<execution::SqlTable *>(ptr_value);
  }

  tuple_id_t TupleId() const {
    return tuple_id_;
  }

  ProjectedRow *Delta() { return reinterpret_cast<ProjectedRow *>(varlen_contents_); }

  const ProjectedRow *Delta() const { return reinterpret_cast<const ProjectedRow *>(varlen_contents_); }

  static uint32_t Size(const ProjectedRowInitializer &initializer) {
    return static_cast<uint32_t>(sizeof(RedoRecordBody)) + initializer.ProjectedRowSize();
  }

  static LogRecord *Initialize(void *head, timestamp_t txn_begin, execution::SqlTable *table, tuple_id_t tuple_id,
                               const ProjectedRowInitializer &initializer) {

    LogRecord *result = LogRecord::InitializeHeader(head,
                                                    LogRecordType::REDO,
                                                    static_cast<uint32_t>(sizeof(RedoRecordBody)
                                                        + initializer.ProjectedRowSize()),
                                                    txn_begin);
    auto *body = result->GetUnderlyingRecordBodyAs<RedoRecordBody>();
    body->table_ = table;
    body->tuple_id_ = tuple_id;
    initializer.InitializeRow(body->varlen_contents_);
    return result;
  }

 private:
  // TODO(Tianyu): We will eventually need to consult the SqlTable to determine how to serialize a given column
  // (varlen? compressed? from an outdated schema?) For now we just assume we can serialize everything out as-is,
  // and the reader still have access to the layout on recovery and can deserialize. This is why we are not
  // just taking an oid.
  execution::SqlTable *table_;
  tuple_id_t tuple_id_;
  // This needs to be aligned to 8 bytes to ensure the real size of RedoRecord (plus actual ProjectedRow) is also
  // a multiple of 8.
  uint64_t varlen_contents_[0];
};

// TODO(Tianyu): Same here
static_assert(sizeof(RedoRecordBody) % 8 == 0,
              "a projected row inside the redo record needs to be aligned to 8 bytes");

class CommitRecordBody {
 public:
  HEAP_ONLY(CommitRecordBody)
};
class CommitRecord : public LogRecord {
 public:
  LogRecordType RecordType() const {
    return LogRecordType::COMMIT;
  }

  void SerializeToLog(LogManager *manager) const;

  timestamp_t CommitTime() const { return txn_commit_; }

  static CommitRecord *Initialize(void *head, timestamp_t txn_begin, timestamp_t txn_commit) {
    auto *result = reinterpret_cast<CommitRecord *>(head);
    result->type_ = LogRecordType::COMMIT;
    result->size_ = static_cast<uint32_t>(sizeof(CommitRecord));
    result->txn_begin_ = txn_begin;
    result->txn_commit_ = txn_commit;
    return result;
  }

 private:
  timestamp_t txn_commit_;
};
}  // terrier::storage