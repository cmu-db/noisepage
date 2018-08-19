#pragma once
#include "common/typedefs.h"
#include "storage/storage_defs.h"

namespace terrier::logging {
/**
 * A LogRecordType is a type of a log record. It describes what operations is
 * recorded.
 */
enum class LogRecordType {
  /** Invalid log records */
  INVALID = -1,

  /** Begins a transaction */
  BEGIN = 1,
  /** Commits a transaction */
  COMMIT = 2,
  /** Aborts a transaction */
  ABORT = 3,

  /** Inserts a tuple */
  INSERT = 11,
  /** Deletes a tuple */
  DELETE = 12,
  /** Updates a tuple */
  UPDATE = 13,
};

/**
 * A LogRecord is a redo record, represents a change included in the transaction.
 */
class LogRecord {
 public:
  /**
   * @brief A LogRecord is only initiated with parameters when created,
   * and should never be copied or moved.
   */
  DISALLOW_COPY_AND_MOVE(LogRecord);

  /**
   * @brief Constructs a LogRecord with the given type, transaction id,
   * commit id, tuple slot, and redo changes.
   *
   * @param type the type of this LogRecord
   * @param txn_id the id of the transaction to which the redo changes belong
   * @param commit_id the commit id of the transaction to which the redo changes belong
   * @param slot the TupleSlot this LogRecord points to
   * @param redo the redo changes this LogRecord represents
   */
  LogRecord(LogRecordType type, timestamp_t txn_id, timestamp_t commit_id, storage::TupleSlot slot,
            const storage::ProjectedRow &redo)
      : type_(type), txn_id_(txn_id), commit_id_(commit_id), slot_(slot) {
    storage::ProjectedRow::InitializeProjectedRow(projected_row_, redo);
  }

  /**
   * @brief Gets the type of the LogRecord
   *
   * @return the type of the LofRecord
   */
  LogRecordType Type() const { return type_; }

  /**
   * @brief Gets the transaction id of the LogRecord
   *
   * @return the transaction id of the LofRecord
   */
  timestamp_t TxnId() const { return txn_id_; }

  /**
   * @brief Gets the commit id of the LogRecord
   *
   * @return the commit id of the LofRecord
   */
  timestamp_t CommitId() const { return commit_id_; }

  /**
   * @brief Gets the tuple slot of the LogRecord
   *
   * @return the tuple slot of the LofRecord
   */
  storage::TupleSlot TupleSlot() const { return slot_; }

  /**
   * @brief Gets the size of the projected row in the LogRecord
   *
   * @return the size of the projected row in the LogRecord
   */
  uint32_t ProjectedRowSize() const {
    return reinterpret_cast<storage::ProjectedRow *>(const_cast<std::byte *>(projected_row_))->Size();
  }

  /**
   * @brief Gets the projected row of the LogRecord
   *
   * @return the projected row of the LofRecord
   */
  std::byte *ProjectedRow() const { return const_cast<std::byte *>(projected_row_); }

 private:
  LogRecordType type_;
  timestamp_t txn_id_;
  timestamp_t commit_id_;
  storage::TupleSlot slot_;
  std::byte projected_row_[0];
};
}  // namespace terrier::logging
