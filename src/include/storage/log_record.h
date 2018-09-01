#pragma once
#include "storage/projected_row.h"
namespace terrier::execution {
class SqlTable;
}  // namespace terrier::execution

namespace terrier::storage {
class LogManager;

// NOLINTNEXTLINE
BETTER_ENUM(LogRecordType, uint8_t, REDO = 1, COMMIT)

class LogRecord {
 public:
  virtual LogRecordType RecordType() const = 0;
  virtual uint32_t Size() const = 0;
  virtual void SerializeToLog(LogManager *manager) const = 0;
  timestamp_t TxnBegin() const { return txn_begin_; }
 protected:
  timestamp_t txn_begin_;
};

class RedoRecord : public LogRecord {
 public:
  RedoRecord() = delete;
  DISALLOW_COPY_AND_MOVE(RedoRecord)
  ~RedoRecord() = delete;

  LogRecordType RecordType() const override {
    return LogRecordType::REDO;
  }

  /**
   * @return size of this RedoRecord in memory, in bytes.
   */
  uint32_t Size() const override { return static_cast<uint32_t>(sizeof(RedoRecord) + Delta()->Size()); }


  execution::SqlTable *SqlTable() const {
    uintptr_t ptr_value = *reinterpret_cast<const uintptr_t *>(varlen_contents_);
    return reinterpret_cast<execution::SqlTable *>(ptr_value);
  }

  tuple_id_t TupleId() const {
    return tuple_id_;
  }

  /**
   * Access the ProjectedRow containing this record's modifications
   * @return pointer to the delta (modifications)
   */
  ProjectedRow *Delta() { return reinterpret_cast<ProjectedRow *>(varlen_contents_); }

  /**
   * Access the ProjectedRow containing this record's modifications
   * @return const pointer to the delta
   */
  const ProjectedRow *Delta() const { return reinterpret_cast<const ProjectedRow *>(varlen_contents_); }

  /**
   * Calculates the size of this RedoRecord, including all members, values, and bitmap
   *
   * @param initializer initializer to use for the embedded ProjectedRow
   * @return number of bytes for this UndoRecord
   */
  static uint32_t Size(const ProjectedRowInitializer &initializer) {
    return static_cast<uint32_t>(sizeof(RedoRecord)) + initializer.ProjectedRowSize();
  }

  static RedoRecord *Initialize(void *head, timestamp_t txn_begin, execution::SqlTable *table, tuple_id_t tuple_id,
                                const ProjectedRowInitializer &initializer) {
    auto* result = reinterpret_cast<RedoRecord *>(head);
    result->txn_begin_ = txn_begin;
    result->table_ = table;
    result->tuple_id_ = tuple_id;
    initializer.InitializeRow(result->varlen_contents_);
    return result;
  }

 private:
  // TODO(Tianyu): We will eventually need to consult the SqlTable to determine how to serialize a given column
  // (varlen? compressed? from an outdated schema?) For now we just assume we can serialize everything out as-is,
  // and the reader still have access to the layout on recovery and can deserialize.
  execution::SqlTable *table_;
  tuple_id_t tuple_id_;
  // This needs to be aligned to 8 bytes to ensure the real size of RedoRecord (plus actual ProjectedRow) is also
  // a multiple of 8.
  uint64_t varlen_contents_[0];
};

static_assert(sizeof(RedoRecord) % 8 == 0,
              "a projected row inside the redo record needs to be aligned to 8 bytes"
              "to ensure true atomicity");

class CommitRecord : public LogRecord {
 public:
  LogRecordType RecordType() const override {
    return LogRecordType::COMMIT;
  }

  /**
   * @return size of this RedoRecord in memory, in bytes.
   */
  uint32_t Size() const override { return static_cast<uint32_t>(sizeof(CommitRecord)); }

  timestamp_t CommitTime() const { return commit_time_; }

 private:
  timestamp_t commit_time_;
};
}  // terrier::storage