#pragma once

#include <ostream>
#include <utility>
#include <vector>
#include "common/constants.h"
#include "common/container/bitmap.h"
#include "common/macros.h"
#include "common/object_pool.h"
#include "common/typedefs.h"

namespace terrier::storage {
/**
 * A block is a chunk of memory used for storage. It does not have any meaning
 * unless interpreted by a @see TupleAccessStrategy
 */
struct RawBlock {
  /**
   * Layout version.
   */
  layout_version_t layout_version_;
  /**
   * Number of records.
   */
  std::atomic<uint32_t> num_records_;
  /**
   * Contents of the raw block.
   */
  byte content_[common::Constants::BLOCK_SIZE - 2 * sizeof(uint32_t)];
  // A Block needs to always be aligned to 1 MB, so we can get free bytes to
  // store offsets within a block in ine 8-byte word.
} __attribute__((aligned(common::Constants::BLOCK_SIZE)));

// TODO(Tianyu): Put this into Constants?
#define MAX_COL INT16_MAX
// TODO(Tianyu): This code eventually should be compiled, which would eliminate
// BlockLayout as a runtime object, instead baking them in as compiled code
// (Think of this as writing the class with a BlockLayout template arg, except
// template instantiation is done by LLVM at runtime and not at compile time.
/**
 * Stores metadata about the layout of a block.
 * This will eventually be baked in as compiled code by LLVM.
 */
struct BlockLayout {
  /**
   * Constructs a new block layout.
   * @param num_attrs number of attributes.
   * @param attr_sizes vector of attribute sizes.
   */
  BlockLayout(const uint16_t num_attrs, std::vector<uint8_t> attr_sizes)
      : num_cols_(num_attrs),
        attr_sizes_(std::move(attr_sizes)),
        tuple_size_(ComputeTupleSize()),
        header_size_(HeaderSize()),
        num_slots_(NumSlots()) {
    PELOTON_ASSERT(num_attrs > 0 && num_attrs <= MAX_COL, "number of columns must be between 1 and 32767");
    PELOTON_ASSERT(num_slots_ != 0, "number of slots cannot be 0!");
  }

  /**
   * Number of columns.
   */
  const uint16_t num_cols_;
  /**
   * Vector of attribute sizes.
   */
  const std::vector<uint8_t> attr_sizes_;
  // Cached tuple size so that we don't have to iterate through attr_sizes_ every time.
  /**
   * Tuple size.
   */
  const uint32_t tuple_size_;
  /**
   * Header size.
   */
  const uint32_t header_size_;
  /**
   * Number of slots in the tuple.
   */
  const uint32_t num_slots_;

 private:
  uint32_t ComputeTupleSize() {
    PELOTON_ASSERT(num_cols_ == attr_sizes_.size(), "Number of attributes does not match number of attribute sizes.");
    uint32_t result = 0;
    for (auto size : attr_sizes_) result += size;
    return result;
  }

  uint32_t HeaderSize() {
    return static_cast<uint32_t>(sizeof(uint32_t) * 3  // layout_version, num_records, num_slots
                                 + num_cols_ * sizeof(uint32_t) + sizeof(uint16_t) + num_cols_ * sizeof(uint8_t));
  }

  uint32_t NumSlots() {
    // Need to account for extra bitmap structures needed for each attribute.
    // TODO(Tianyu): I am subtracting 1 from this number so we will always have
    // space to pad each individual bitmap to full bytes (every attribute is
    // at least a byte). Somebody can come and fix this later, because I don't
    // feel like thinking about this now.
    return 8 * (common::Constants::BLOCK_SIZE - header_size_) / (8 * tuple_size_ + num_cols_) - 2;
  }
};

/**
 * A TupleSlot represents a physical location of a tuple in memory.
 */
class TupleSlot {
 public:
  /**
   * Constructs an empty tuple slot
   */
  TupleSlot() : bytes_(0) {}

  /**
   * Construct a tuple slot representing the given offset in the given block
   * @param block the block this slot is in
   * @param offset the offset of this slot in its block
   */
  TupleSlot(RawBlock *block, uint32_t offset) : bytes_(reinterpret_cast<uintptr_t>(block) | offset) {
    PELOTON_ASSERT(!((static_cast<uintptr_t>(common::Constants::BLOCK_SIZE) - 1) & ((uintptr_t)block)),
                   "Address must be aligned to block size (last bits zero).");
    PELOTON_ASSERT(offset < common::Constants::BLOCK_SIZE,
                   "Offset must be smaller than block size (to fit in the last bits).");
  }

  /**
   * @return ptr to the head of the block
   */
  RawBlock *GetBlock() const {
    // Get the first 11 bytes as the ptr
    return reinterpret_cast<RawBlock *>(bytes_ & ~(static_cast<uintptr_t>(common::Constants::BLOCK_SIZE) - 1));
  }

  /**
   * @return offset of the tuple within a block.
   */
  uint32_t GetOffset() const {
    return static_cast<uint32_t>(bytes_ & (static_cast<uintptr_t>(common::Constants::BLOCK_SIZE) - 1));
  }

  /**
   * Checks if this TupleSlot is equal to the other.
   * @param other the other TupleSlot to be compared.
   * @return true if the TupleSlots are equal, false otherwise.
   */
  bool operator==(const TupleSlot &other) const { return bytes_ == other.bytes_; }

  /**
   * Checks if this TupleSlot is not equal to the other.
   * @param other the other TupleSlot to be compared.
   * @return true if the TupleSlots are not equal, false otherwise.
   */
  bool operator!=(const TupleSlot &other) const { return bytes_ != other.bytes_; }

  /**
   * Outputs the TupleSlot to the output stream.
   * @param os output stream to be written to.
   * @param slot TupleSlot to be output.
   * @return the modified output stream.
   */
  friend std::ostream &operator<<(std::ostream &os, const TupleSlot &slot) {
    return os << "block: " << slot.GetBlock() << ", offset: " << slot.GetOffset();
  }

 private:
  friend struct std::hash<TupleSlot>;
  // Block pointers are always aligned to 1 mb, thus we get 5 free bytes to
  // store the offset.
  uintptr_t bytes_{0};
};

/**
 * Allocator that allocates a block
 */
struct BlockAllocator {
  /**
   * Allocates a new object by calling its constructor.
   * @return a pointer to the allocated object.
   */
  RawBlock *New() { return new RawBlock(); }

  /**
   * Reuse a reused chunk of memory to be handed out again
   * @param reused memory location, possibly filled with junk bytes
   */
  void Reuse(RawBlock *reused) { /* no operation required */
  }

  /**
   * Deletes the object by calling its destructor.
   * @param ptr a pointer to the object to be deleted.
   */
  void Delete(RawBlock *ptr) { delete ptr; }
};

/**
 * A block store is essentially an object pool. However, all blocks should be
 * aligned, so we will need to use the default constructor instead of raw
 * malloc.
 */
using BlockStore = common::ObjectPool<RawBlock, BlockAllocator>;

// TODO(Tianyu): Align this
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
  static ProjectedRow *InitializeProjectedRow(void *head, const ProjectedRow &other) {
    auto *result = reinterpret_cast<ProjectedRow *>(head);
    auto header_size =
        static_cast<uint32_t>(sizeof(ProjectedRow) + +other.num_cols_ * (sizeof(uint16_t) + sizeof(uint32_t)));
    // TODO(Tianyu): Pretty sure I can just mem-cpy the header?
    PELOTON_MEMCPY(result, &other, header_size);
    result->Bitmap().Clear(result->num_cols_);
    return result;
  }

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
class DeltaRecord {
 public:
  DeltaRecord() = delete;
  DISALLOW_COPY_AND_MOVE(DeltaRecord)
  ~DeltaRecord() = delete;

  /**
   * @return Pointer to the next element in the version chain
   */
  DeltaRecord *&Next() { return next_; }

  /**
   * @return Timestamp up to which the old projected row was visible.
   */
  std::atomic<timestamp_t> &Timestamp() { return timestamp_; }

  /**
   * @return the DataTable this DeltaRecord points to
   */
  DataTable *Table() { return table_; }

  /**
   * @return the TupleSlot this DeltaRecord points to
   */
  TupleSlot Slot() { return slot_; }

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
  uint32_t Size() { return static_cast<uint32_t>(sizeof(DeltaRecord) + Delta()->Size()); }

  /**
   * @param redo the redo changes to be applied
   * @return size of the DeltaRecord which can store the delta resulting from applying redo in memory, in bytes
   */
  static uint32_t Size(const ProjectedRow &redo) { return static_cast<uint32_t>(sizeof(DeltaRecord)) + redo.Size(); }

  /**
   * Calculates the size of this DeltaRecord, including all members, values, and bitmap
   *
   * @param layout BlockLayout of the RawBlock to be accessed
   * @param col_ids projection list of column ids to map
   * @return number of bytes for this DeltaRecord
   */
  static uint32_t Size(const BlockLayout &layout, const std::vector<uint16_t> &col_ids) {
    return static_cast<uint32_t>(sizeof(DeltaRecord)) + ProjectedRow::Size(layout, col_ids);
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
  static DeltaRecord *InitializeDeltaRecord(void *head, timestamp_t timestamp, TupleSlot slot, DataTable *table,
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
  static DeltaRecord *InitializeDeltaRecord(void *head, timestamp_t timestamp, TupleSlot slot, DataTable *table,
                                            const storage::ProjectedRow &redo) {
    auto *result = reinterpret_cast<DeltaRecord *>(head);

    result->next_ = nullptr;
    result->timestamp_.store(timestamp);
    result->table_ = table;
    result->slot_ = slot;

    ProjectedRow::InitializeProjectedRow(result->varlen_contents_, redo);

    return result;
  }

 private:
  // TODO(Tianyu): Always padded?
  DeltaRecord *next_;
  std::atomic<timestamp_t> timestamp_;
  DataTable *table_;
  TupleSlot slot_;
  byte varlen_contents_[0];
};

static_assert(sizeof(DeltaRecord) == 32, "must be aligned");
}  // namespace terrier::storage

namespace std {
/**
 * Implements std::hash for TupleSlot.
 */
template <>
struct hash<terrier::storage::TupleSlot> {
  /**
   * Returns the hash of the slot's contents.
   * @param slot the slot to be hashed.
   * @return the hash of the slot.
   */
  size_t operator()(const terrier::storage::TupleSlot &slot) const { return hash<uintptr_t>()(slot.bytes_); }
};
}  // namespace std
