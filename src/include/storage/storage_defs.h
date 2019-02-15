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
 */
struct alignas(common::Constants::BLOCK_SIZE) RawBlock {
  /**
   * Layout version.
   */
  layout_version_t layout_version_;
  /**
   * The insert head tells us where the next insertion should take place. Notice that this counter is never
   * decreased as slot recycling does not happen on the fly with insertions. A background compaction process
   * scans through blocks and free up slots.
   */
  std::atomic<uint32_t> insert_head_;
  /**
   * Contents of the raw block.
   */
  byte content_[common::Constants::BLOCK_SIZE - 2 * sizeof(uint32_t)];
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
/**
 * Used by SqlTable to map between col_oids in Schema and col_ids in BlockLayout
 */
using ColumnMap = std::unordered_map<catalog::col_oid_t, col_id_t>;
/**
 * Used by execution and storage layers to map between col_oids and offsets within a ProjectedRow
 */
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
 * A varlen entry is always a 32-bit size field and the varlen content,
 * with exactly size many bytes (no extra nul in the end).
 */
class VarlenEntry {
 public:
  // Have to define a default constructor to make this POD
  VarlenEntry() = default;
  /**
   * Constructs a new varlen entry
   * @param content pointer to the varlen content itself
   * @param size length of the varlen content, in bytes (no C-style nul-terminator)
   * @param gathered whether the varlen entry's content pointer is part of a large buffer (for arrow-compatibility),
   *                 which means it cannot be deallocated by itself.
   */
  VarlenEntry(byte *content, uint32_t size, bool gathered)
      // the sign bit on size is used to store the "gathered" attribute, so we mask it off on size depending on that.
      : size_(size | (gathered ? INT32_MIN : 0)), content_(content) {}
  /**
   * @return size of the varlen entry in bytes.
   */
  uint32_t Size() const { return static_cast<uint32_t>(INT32_MAX & size_); }

  /**
   * @return whether the varlen is gathered into a per-block contiguous buffer (which means it cannot be
   * deallocated by itself) for arrow-compatibility
   */
  bool IsGathered() const { return static_cast<bool>(INT32_MIN & size_); }

  /**
   * @return pointer to the varlen entry contents.
   */
  const byte *Content() const { return content_; }

 private:
  // we use the sign bit to denote if
  int32_t size_;
  // TODO(Tianyu): we can use the extra 4 bytes for something else (storing the prefix?)
  // Contents of the varlen entry.
  const byte *content_;
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
