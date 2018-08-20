#pragma once
#include <vector>
#include "common/macros.h"
#include "storage/storage_defs.h"

namespace terrier::storage {
// TODO(Tianyu): Align the values
/**
 * A projected row is a partial row image of a tuple. It also encodes
 * a projection list that allows for reordering of the columns. Its in-memory
 * layout:
 * -------------------------------------------------------------------------------
 * | size | num_cols | col_id1 | col_id2 | ... | val1_offset | val2_offset | ... |
 * -------------------------------------------------------------------------------
 * | null-bitmap (pad up to byte) | val1 | val2 | ...                            |
 * -------------------------------------------------------------------------------
 * Warning, 0 means null in the null-bitmap
 *
 * The projection list is encoded as position of col_id -> col_id. For example:
 *
 * --------------------------------------------------------
 * | 36 | 3 | 1 | 0 | 2 | 0 | 4 | 8 | 0xC0 | 721 | 15 | x |
 * --------------------------------------------------------
 * Would be the row: { 0 -> 15, 1 -> 721, 2 -> nul}
 */
// TODO(Tianyu): The PACKED directive here is not necessary, but C++ does some weird thing where
// it will pad sizeof(ProjectedRow) to 8 but the varlen content still points at 6. This should not
// break any code, but the padding no will be immediately before the values instead of after num_cols,
// which is weird. We should make a consistent policy on how to deal with this type of issue for other
// cases as well. (The other use cases of byte[0] all have aligned sizes already I think?)
class PACKED ProjectedRow {
 public:
  // A ProjectedRow should only be reinterpreted from raw bytes on the heap
  ProjectedRow() = delete;
  DISALLOW_COPY_AND_MOVE(ProjectedRow)
  ~ProjectedRow() = delete;

  /**
   * Calculates the size of this ProjectedRow, including all members, values, bitmap, and potential padding
   * @param layout BlockLayout of the RawBlock to be accessed
   * @param col_ids projection list of column ids to map
   * @return number of bytes for this ProjectedRow
   */
  static uint32_t Size(const BlockLayout &layout, const std::vector<uint16_t> &col_ids);

  /**
   * Populates the ProjectedRow's members based on projection list and BlockLayout
   * @param head pointer to the byte buffer to initialize as a ProjectedRow
   * @param col_ids projection list of column ids to map
   * @param layout BlockLayout of the RawBlock to be accessed
   * @return pointer to the initialized ProjectedRow
   */
  static ProjectedRow *InitializeProjectedRow(void *head, const std::vector<uint16_t> &col_ids,
                                              const BlockLayout &layout);

  /**
   * Populates the ProjectedRow's members based on an existing ProjectedRow. The new ProjectRow has the
   * same layout as the given one.
   *
   * @param head pointer to the byte buffer to initialize as a ProjectedRow
   * @param other ProjectedRow to use as template for setup
   * @return pointer to the initialized ProjectedRow
   */
  static ProjectedRow *InitializeProjectedRow(void *head, const ProjectedRow &other);

  /**
   * @return the size of this ProjectedRow in memory, in bytes
   */
  uint32_t Size() const { return size_; }

  /**
   * @return number of columns stored in the ProjectedRow
   */
  uint16_t NumColumns() const { return num_cols_; }

  /**
   * @return pointer to the start of the uint16_t array of column ids
   */
  uint16_t *ColumnIds() { return reinterpret_cast<uint16_t *>(varlen_contents_); }

  /**
   * @return pointer to the start of the uint16_t array of column ids
   */
  const uint16_t *ColumnIds() const { return reinterpret_cast<const uint16_t *>(varlen_contents_); }

  /**
   * Access a single attribute within the ProjectedRow with a check of the null bitmap first for nullable types
   * @param offset The 0-indexed element to access in this ProjectedRow
   * @return byte pointer to the attribute. reinterpret_cast and dereference to access the value. if attribute is
   * nullable and set to null, then return value is nullptr
   */
  byte *AccessWithNullCheck(const uint16_t offset) {
    PELOTON_ASSERT(offset < num_cols_, "Column offset out of bounds.");
    if (!Bitmap().Test(offset)) return nullptr;
    return reinterpret_cast<byte *>(this) + AttrValueOffsets()[offset];
  }

  /**
   * Access a single attribute within the ProjectedRow with a check of the null bitmap first for nullable types
   * @param offset The 0-indexed element to access in this ProjectedRow
   * @return byte pointer to the attribute. reinterpret_cast and dereference to access the value. if attribute is
   * nullable and set to null, then return value is nullptr
   */
  const byte *AccessWithNullCheck(const uint16_t offset) const {
    PELOTON_ASSERT(offset < num_cols_, "Column offset out of bounds.");
    if (!Bitmap().Test(offset)) return nullptr;
    return reinterpret_cast<const byte *>(this) + AttrValueOffsets()[offset];
  }

  /**
   * Access a single attribute within the ProjectedRow without a check of the null bitmap first
   * @param offset The 0-indexed element to access in this ProjectedRow
   * @return byte pointer to the attribute. reinterpret_cast and dereference to access the value
   */
  byte *AccessForceNotNull(const uint16_t offset) {
    PELOTON_ASSERT(offset < num_cols_, "Column offset out of bounds.");
    if (!Bitmap().Test(offset)) Bitmap().Flip(offset);
    return reinterpret_cast<byte *>(this) + AttrValueOffsets()[offset];
  }

  /**
   * Set the attribute in the ProjectedRow to be null using the internal bitmap
   * @param offset The 0-indexed element to access in this ProjectedRow
   */
  void SetNull(const uint16_t offset) {
    PELOTON_ASSERT(offset < num_cols_, "Column offset out of bounds.");
    Bitmap().Set(offset, false);
  }

  /**
   * Set the attribute in the ProjectedRow to be not null using the internal bitmap
   * @param offset The 0-indexed element to access in this ProjectedRow
   */
  void SetNotNull(const uint16_t offset) {
    PELOTON_ASSERT(offset < num_cols_, "Column offset out of bounds.");
    Bitmap().Set(offset, true);
  }

 private:
  uint32_t size_;
  uint16_t num_cols_;
  byte varlen_contents_[0];

  uint32_t *AttrValueOffsets() { return reinterpret_cast<uint32_t *>(ColumnIds() + num_cols_); }

  const uint32_t *AttrValueOffsets() const { return reinterpret_cast<const uint32_t *>(ColumnIds() + num_cols_); }

  common::RawBitmap &Bitmap() { return *reinterpret_cast<common::RawBitmap *>(AttrValueOffsets() + num_cols_); }

  const common::RawBitmap &Bitmap() const {
    return *reinterpret_cast<const common::RawBitmap *>(AttrValueOffsets() + num_cols_);
  }
};

class DataTable;
/**
 * Extension of a ProjectedRow that adds two additional fields: a timestamp and a pointer to the next entry in the
 * version chain
 */
class UndoRecord {
 public:
  UndoRecord() = delete;
  DISALLOW_COPY_AND_MOVE(UndoRecord)
  ~UndoRecord() = delete;

  /**
   * @return Pointer to the next element in the version chain
   */
  std::atomic<UndoRecord *> &Next() { return next_; }

  /**
   * @return Timestamp up to which the old projected row was visible.
   */
  std::atomic<timestamp_t> &Timestamp() { return timestamp_; }

  /**
   * @return Timestamp up to which the old projected row was visible.
   */
  const std::atomic<timestamp_t> &Timestamp() const { return timestamp_; }

  /**
   * @return the DataTable this DeltaRecord points to
   */
  DataTable *Table() const { return table_; }

  /**
   * @return the TupleSlot this DeltaRecord points to
   */
  TupleSlot Slot() const { return slot_; }

  /**
   * Access the ProjectedRow containing this record's modifications
   * @return pointer to the delta (modifications)
   */
  ProjectedRow *Delta() { return reinterpret_cast<ProjectedRow *>(varlen_contents_); }

  /**
   * Access the next version in the delta chain
   * @return pointer to the next version
   */
  const ProjectedRow *Delta() const { return reinterpret_cast<const ProjectedRow *>(varlen_contents_); }

  /**
   * @return size of this DeltaRecord in memory, in bytes.
   */
  uint32_t Size() { return static_cast<uint32_t>(sizeof(UndoRecord) + Delta()->Size()); }

  /**
   * @param redo the redo changes to be applied
   * @return size of the DeltaRecord which can store the delta resulting from applying redo in memory, in bytes
   */
  static uint32_t Size(const ProjectedRow &redo) { return static_cast<uint32_t>(sizeof(UndoRecord)) + redo.Size(); }

  /**
   * Calculates the size of this DeltaRecord, including all members, values, and bitmap
   *
   * @param layout BlockLayout of the RawBlock to be accessed
   * @param col_ids projection list of column ids to map
   * @return number of bytes for this DeltaRecord
   */
  static uint32_t Size(const BlockLayout &layout, const std::vector<uint16_t> &col_ids) {
    return static_cast<uint32_t>(sizeof(UndoRecord)) + ProjectedRow::Size(layout, col_ids);
  }

  /**
   * Populates the DeltaRecord's members based on next pointer, timestamp, projection list, and BlockLayout.
   *
   * @param head pointer to the byte buffer to initialize as a DeltaRecord
   * @param timestamp timestamp of the transaction that generated this DeltaRecord
   * @param slot the TupleSlot this DeltaRecord points to
   * @param table the DataTable this DeltaRecord points to
   * @param layout BlockLayout of the RawBlock to be accessed
   * @param col_ids projection list of column ids to map
   * @return pointer to the initialized DeltaRecord
   */
  static UndoRecord *InitializeDeltaRecord(void *head, timestamp_t timestamp, TupleSlot slot, DataTable *table,
                                           const BlockLayout &layout, const std::vector<uint16_t> &col_ids);

  /**
   * Populates the DeltaRecord's members based on next pointer, timestamp, projection list, and the redo changes that
   * this DeltaRecord is supposed to log.
   *
   * @param head pointer to the byte buffer to initialize as a DeltaRecord
   * @param timestamp timestamp of the transaction that generated this DeltaRecord
   * @param slot the TupleSlot this DeltaRecord points to
   * @param table the DataTable this DeltaRecord points to
   * @param redo the redo changes to be applied
   * @return pointer to the initialized DeltaRecord
   */
  static UndoRecord *InitializeDeltaRecord(void *head, timestamp_t timestamp, TupleSlot slot, DataTable *table,
                                           const storage::ProjectedRow &redo) {
    auto *result = reinterpret_cast<UndoRecord *>(head);

    result->next_ = nullptr;
    result->timestamp_.store(timestamp);
    result->table_ = table;
    result->slot_ = slot;

    ProjectedRow::InitializeProjectedRow(result->varlen_contents_, redo);

    return result;
  }

 private:
  std::atomic<UndoRecord *> next_;
  std::atomic<timestamp_t> timestamp_;
  DataTable *table_;
  TupleSlot slot_;
  byte varlen_contents_[0];
};

static_assert(sizeof(UndoRecord) % 8 == 0, "a projected row inside the undo record needs to be aligned to 8 bytes");
}  // namespace terrier::storage
