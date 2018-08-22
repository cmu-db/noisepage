#pragma once
#include <vector>
#include "common/macros.h"
#include "storage/storage_defs.h"
#include "storage/storage_util.h"

namespace terrier::storage {
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
// The PACKED directive here is not necessary, but C++ does some weird thing where it will pad sizeof(ProjectedRow)
// to 8 but the varlen content still points at 6. This should not break any code, but the padding now will be
// immediately before the values instead of after num_cols, which is weird.
//
// This should not have any impact because we disallow direct construction of a ProjectedRow and
// we will have control over the exact padding and layout by allocating byte arrays on the heap
// and then initializing using the static factory provided.
class PACKED ProjectedRow {
 public:
  // A ProjectedRow should only be reinterpreted from raw bytes on the heap
  ProjectedRow() = delete;
  DISALLOW_COPY_AND_MOVE(ProjectedRow)
  ~ProjectedRow() = delete;

  /**
   * Populates the ProjectedRow's members based on an existing ProjectedRow. The new ProjectRow has the
   * same layout as the given one.
   *
   * @param head pointer to the byte buffer to initialize as a ProjectedRow
   * @param other ProjectedRow to use as template for setup
   * @return pointer to the initialized ProjectedRow
   */
  static ProjectedRow *CopyProjectedRowLayout(void *head, const ProjectedRow &other);

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
    TERRIER_ASSERT(offset < num_cols_, "Column offset out of bounds.");
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
    TERRIER_ASSERT(offset < num_cols_, "Column offset out of bounds.");
    if (!Bitmap().Test(offset)) return nullptr;
    return reinterpret_cast<const byte *>(this) + AttrValueOffsets()[offset];
  }

  /**
   * Access a single attribute within the ProjectedRow without a check of the null bitmap first
   * @param offset The 0-indexed element to access in this ProjectedRow
   * @return byte pointer to the attribute. reinterpret_cast and dereference to access the value
   */
  byte *AccessForceNotNull(const uint16_t offset) {
    TERRIER_ASSERT(offset < num_cols_, "Column offset out of bounds.");
    if (!Bitmap().Test(offset)) Bitmap().Flip(offset);
    return reinterpret_cast<byte *>(this) + AttrValueOffsets()[offset];
  }

  /**
   * Set the attribute in the ProjectedRow to be null using the internal bitmap
   * @param offset The 0-indexed element to access in this ProjectedRow
   */
  void SetNull(const uint16_t offset) {
    TERRIER_ASSERT(offset < num_cols_, "Column offset out of bounds.");
    Bitmap().Set(offset, false);
  }

  /**
   * Set the attribute in the ProjectedRow to be not null using the internal bitmap
   * @param offset The 0-indexed element to access in this ProjectedRow
   */
  void SetNotNull(const uint16_t offset) {
    TERRIER_ASSERT(offset < num_cols_, "Column offset out of bounds.");
    Bitmap().Set(offset, true);
  }

 private:
  friend class ProjectedRowInitializer;
  uint32_t size_;
  uint16_t num_cols_;
  byte varlen_contents_[0];

  uint32_t *AttrValueOffsets() { return StorageUtil::AlignedPtr<uint32_t>(ColumnIds() + num_cols_); }

  const uint32_t *AttrValueOffsets() const { return StorageUtil::AlignedPtr<const uint32_t>(ColumnIds() + num_cols_); }

  common::RawBitmap &Bitmap() { return *reinterpret_cast<common::RawBitmap *>(AttrValueOffsets() + num_cols_); }

  const common::RawBitmap &Bitmap() const {
    return *reinterpret_cast<const common::RawBitmap *>(AttrValueOffsets() + num_cols_);
  }
};

class ProjectedRowInitializer {
 public:
  /**
   * Constructs a ProjectedRowInitializer. Calculates the size of this ProjectedRow, including all members, values,
   * bitmap, and potential padding, and the offsets to jump to for each value. This information is cached for repeated
   * initialization.
   * @param layout BlockLayout of the RawBlock to be accessed
   * @param col_ids projection list of column ids to map
   */
  ProjectedRowInitializer(const BlockLayout &layout, std::vector<uint16_t> col_ids);

  /**
   * Populates the ProjectedRow's members based on projection list and BlockLayout used to construct this initializer
   * @param head pointer to the byte buffer to initialize as a ProjectedRow
   * @return pointer to the initialized ProjectedRow
   */
  ProjectedRow *InitializeProjectedRow(void *head) const;

  /**
   * @return size of the ProjectedRow in memory, in bytes, that this initializer constructs.
   */
  uint32_t ProjectedRowSize() const { return size_; }

 private:
  uint32_t size_ = 0;
  std::vector<uint16_t> col_ids_;
  std::vector<uint32_t> offsets_;
};

class DataTable;
/**
 * Extension of a ProjectedRow that adds relevant information to be able to traverse the version chain and find the
 * relevant tuple version:
 * pointer to the next record, timestamp of the transaction that created this record, pointer to the data table, and the
 * tuple slot.
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
  static UndoRecord *InitializeRecord(void *head, timestamp_t timestamp, TupleSlot slot, DataTable *table,
                                      const ProjectedRowInitializer &initializer);

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
  static UndoRecord *InitializeRecord(void *head, timestamp_t timestamp, TupleSlot slot, DataTable *table,
                                      const storage::ProjectedRow &redo) {
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
  byte varlen_contents_[0];
};

static_assert(sizeof(UndoRecord) % 8 == 0,
              "a projected row inside the undo record needs to be aligned to 8 bytes"
              "to ensure true atomicity");
}  // namespace terrier::storage
