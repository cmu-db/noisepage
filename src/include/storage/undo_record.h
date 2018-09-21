#pragma once
#include <vector>
#include "storage/projected_row.h"

namespace terrier::storage {
class DataTable;
/**
 * Extension of a ProjectedRow that adds relevant information to be able to traverse the version chain and find the
 * relevant tuple version:
 * pointer to the next record, timestamp of the transaction that created this record, pointer to the data table, and the
 * tuple slot.
 */
class UndoRecord {
 public:
  MEM_REINTERPRETAION_ONLY(UndoRecord)

  /**
   * @return Pointer to the next element in the version chain
   */
  std::atomic<UndoRecord *> &Next() { return next_; }

  /**
   * @return const Pointer to the next element in the version chain
   */
  const std::atomic<UndoRecord *> &Next() const { return next_; }

  /**
   * @return Timestamp up to which the old projected row was visible.
   */
  std::atomic<timestamp_t> &Timestamp() { return timestamp_; }

  /**
   * @return Timestamp up to which the old projected row was visible.
   */
  const std::atomic<timestamp_t> &Timestamp() const { return timestamp_; }

  /**
   * @return the DataTable this UndoRecord points to
   */
  DataTable *Table() const { return table_; }

  /**
   * @return the TupleSlot this UndoRecord points to
   */
  TupleSlot Slot() const { return slot_; }

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
   * @return size of this UndoRecord in memory, in bytes.
   */
  uint32_t Size() const { return static_cast<uint32_t>(sizeof(UndoRecord) + Delta()->Size()); }

  /**
   * @param redo the redo changes to be applied
   * @return size of the UndoRecord which can store the delta resulting from applying redo in memory, in bytes
   */
  static uint32_t Size(const ProjectedRow &redo) { return static_cast<uint32_t>(sizeof(UndoRecord)) + redo.Size(); }

  /**
   * Calculates the size of this UndoRecord, including all members, values, and bitmap
   *
   * @param initializer initializer to use for the embedded ProjectedRow
   * @return number of bytes for this UndoRecord
   */
  static uint32_t Size(const ProjectedRowInitializer &initializer) {
    return static_cast<uint32_t>(sizeof(UndoRecord)) + initializer.ProjectedRowSize();
  }

  /**
   * Populates the UndoRecord's members based on next pointer, timestamp, projection list, and BlockLayout.
   *
   * @param head pointer to the byte buffer to initialize as a UndoRecord
   * @param timestamp timestamp of the transaction that generated this UndoRecord
   * @param slot the TupleSlot this UndoRecord points to
   * @param table the DataTable this UndoRecord points to
   * @param initializer the initializer to use for the embedded ProjectedRow
   * @return pointer to the initialized UndoRecord
   */
  static UndoRecord *Initialize(byte *const head, const timestamp_t timestamp, const TupleSlot slot,
                                DataTable *const table, const ProjectedRowInitializer &initializer) {
    auto *result = reinterpret_cast<UndoRecord *>(head);

    result->next_ = nullptr;
    result->timestamp_.store(timestamp);
    result->table_ = table;
    result->slot_ = slot;

    initializer.InitializeRow(result->varlen_contents_);

    return result;
  }

  /**
   * Populates the UndoRecord's members based on next pointer, timestamp, projection list, and the redo changes that
   * this UndoRecord is supposed to log.
   *
   * @param head pointer to the byte buffer to initialize as a UndoRecord
   * @param timestamp timestamp of the transaction that generated this UndoRecord
   * @param slot the TupleSlot this UndoRecord points to
   * @param table the DataTable this UndoRecord points to
   * @param redo the redo changes to be applied
   * @return pointer to the initialized UndoRecord
   */
  static UndoRecord *Initialize(byte *const head, const timestamp_t timestamp, const TupleSlot slot,
                                DataTable *const table, const storage::ProjectedRow &redo) {
    auto *result = reinterpret_cast<UndoRecord *>(head);

    result->next_ = nullptr;
    result->timestamp_.store(timestamp);
    result->table_ = table;
    result->slot_ = slot;

    ProjectedRow::CopyProjectedRowLayout(result->varlen_contents_, redo);

    return result;
  }

 private:
  std::atomic<UndoRecord *> next_;
  std::atomic<timestamp_t> timestamp_;
  DataTable *table_;
  TupleSlot slot_;
  // This needs to be aligned to 8 bytes to ensure the real size of UndoRecord (plus actual ProjectedRow) is also
  // a multiple of 8.
  uint64_t varlen_contents_[0];
};

static_assert(sizeof(UndoRecord) % 8 == 0,
              "a projected row inside the undo record needs to be aligned to 8 bytes"
              "to ensure true atomicity");

}  // namespace terrier::storage
