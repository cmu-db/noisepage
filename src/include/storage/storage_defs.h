#pragma once

#include <algorithm>
#include <functional>
#include <ostream>
#include <unordered_map>
#include <utility>
#include <vector>
#include "catalog/catalog_defs.h"
#include "common/constants.h"
#include "common/container/bitmap.h"
#include "common/macros.h"
#include "common/object_pool.h"
#include "common/strong_typedef.h"
#include "storage/block_access_controller.h"

namespace terrier::storage {
// Write Ahead Logging:
#define LOGGING_DISABLED nullptr

// All tuples potentially visible to txns should have a non-null attribute of version vector.
// This is not to be confused with a non-null version vector that has value nullptr (0).
#define VERSION_POINTER_COLUMN_ID ::terrier::storage::col_id_t(0)
#define NUM_RESERVED_COLUMNS 1u

STRONG_TYPEDEF(col_id_t, uint16_t);
STRONG_TYPEDEF(layout_version_t, uint32_t);

/**
 * A block is a chunk of memory used for storage. It does not have any meaning
 * unless interpreted by a @see TupleAccessStrategy
 *
 * @warning If you change this please also change the way header sizes are computed in block layout!
 */
struct alignas(common::Constants::BLOCK_SIZE) RawBlock {
  /**
   * Layout version.
   */
  layout_version_t layout_version_;
  /**
   * Number of records.
   */
  std::atomic<uint32_t> insert_head_;
  /**
   * Access controller of this block
   */
  BlockAccessController controller_;
  /**
   * Contents of the raw block.
   */
  byte content_[common::Constants::BLOCK_SIZE - 2 * sizeof(uint32_t) - sizeof(BlockAccessController)];
  // A Block needs to always be aligned to 1 MB, so we can get free bytes to
  // store offsets within a block in ine 8-byte word.
};

/**
 * A TupleSlot represents a physical location of a tuple in memory.
 */
class TupleSlot {
 public:
  /**
   * Constructs an empty tuple slot (uninitialized)
   */
  TupleSlot() = default;

  /**
   * Construct a tuple slot representing the given offset in the given block
   * @param block the block this slot is in
   * @param offset the offset of this slot in its block
   */
  TupleSlot(const RawBlock *const block, const uint32_t offset) : bytes_(reinterpret_cast<uintptr_t>(block) | offset) {
    TERRIER_ASSERT(!((static_cast<uintptr_t>(common::Constants::BLOCK_SIZE) - 1) & ((uintptr_t)block)),
                   "Address must be aligned to block size (last bits zero).");
    TERRIER_ASSERT(offset < common::Constants::BLOCK_SIZE,
                   "Offset must be smaller than block size (to fit in the last bits).");
  }

  /**
   * @return ptr to the head of the block
   */
  RawBlock *GetBlock() const {
    // Get the first 44 bits as the ptr
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
  uintptr_t bytes_;
};

/**
 * Allocator that allocates a block
 */
class BlockAllocator {
 public:
  /**
   * Allocates a new object by calling its constructor.
   * @return a pointer to the allocated object.
   */
  RawBlock *New() { return new RawBlock(); }

  /**
   * Reuse a reused chunk of memory to be handed out again
   * @param reused memory location, possibly filled with junk bytes
   */
  void Reuse(RawBlock *const reused) { /* no operation required */
  }

  /**
   * Deletes the object by calling its destructor.
   * @param ptr a pointer to the object to be deleted.
   */
  void Delete(RawBlock *const ptr) { delete ptr; }
};

/**
 * A block store is essentially an object pool. However, all blocks should be
 * aligned, so we will need to use the default constructor instead of raw
 * malloc.
 */
using BlockStore = common::ObjectPool<RawBlock, BlockAllocator>;

using ColumnMap = std::unordered_map<catalog::col_oid_t, col_id_t>;
using ProjectionMap = std::unordered_map<catalog::col_oid_t, uint16_t>;

/**
 * Denote whether a record modifies the logical delete column, used when DataTable inspects deltas
 * TODO(Matt): could be used by the GC for recycling
 */
enum class DeltaRecordType : uint8_t { UPDATE = 0, INSERT, DELETE };

/**
 * Types of LogRecords
 */
enum class LogRecordType : uint8_t { REDO = 1, DELETE, COMMIT };
/**
 * A varlen entry is stored as a pointer and a size in the data table.
 *
 */
// TODO(Tianyu): This is pretty wasteful. While in theory 4 bytes of size suffices, we pad it to 8 bytes for
// performance and ease of implementation with the rest of the system. (It is always assumed that one SQL level column
// is mapped to one data table column). In the long run though, we might want to investigate solutions where the varlen
// pointer and the size columns are stored in separate columns, so the size column can be 4 bytes.
class VarlenEntry {
 public:
  byte *GetValue() const{
    return content_;
  }

  uint64_t GetSize() const {
    // We mask off the first sign bit, as that is used to denote whether a varlen is inlined
    // in a contiguous buffer per block for arrow, which requires different GC behavior
    return size_ & INT64_MAX;
  }

  bool IsInlined() const {
    return static_cast<int64_t>(size_) < 0;
  }

  // TODO(Tianyu): Setters need to be implemented
 private:
  byte *content_;
  uint64_t size_;
};

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
