#pragma once

#include <algorithm>
#include <functional>
#include <ostream>
#include <unordered_map>
#include <utility>

#include "catalog/catalog_defs.h"
#include "common/constants.h"
#include "common/macros.h"
#include "common/object_pool.h"
#include "common/strong_typedef.h"
#include "execution/sql/sql.h"
#include "storage/block_access_controller.h"
#include "transaction/transaction_defs.h"

namespace noisepage::storage {

// Internally we use the sign bit to represent if a column is varlen or not. Down to the implementation detail though,
// we always allocate 16 bytes for a varlen entry, with the first 8 bytes being the pointer to the value and following
// 4 bytes be the size of the varlen. There are 4 bytes of padding for alignment purposes.
constexpr uint16_t VARLEN_COLUMN = static_cast<uint16_t>(0x8010);  // 16 with the first (most significant) bit set to 1

// In type_util.h there are a total of 5 possible inlined attribute sizes:
// 1, 2, 4, 8, and 16-bytes (16 byte is the structure portion of varlen).
// Since we pack these attributes in descending size order, we can infer a
// columns size by tracking the locations of the attribute size boundaries.
// Therefore, we only need to track 4 locations because the exterior bounds
// are implicit.
constexpr uint8_t NUM_ATTR_BOUNDARIES = 4;

STRONG_TYPEDEF_HEADER(col_id_t, uint16_t);
STRONG_TYPEDEF_HEADER(layout_version_t, uint16_t);

// All tuples potentially visible to txns should have a non-null attribute of version vector.
// This is not to be confused with a non-null version vector that has value nullptr (0).

constexpr col_id_t VERSION_POINTER_COLUMN_ID = col_id_t(0);
constexpr uint8_t NUM_RESERVED_COLUMNS = 1;

class DataTable;

/**
 * A block is a chunk of memory used for storage. It does not have any meaning
 * unless interpreted by a TupleAccessStrategy. The header layout is documented in the class as well.
 * @see TupleAccessStrategy
 *
 * @warning If you change the layout please also change the way header sizes are computed in block layout!
 */
class alignas(common::Constants::BLOCK_SIZE) RawBlock {
 public:
  /**
   * Data Table for this RawBlock. This is used by indexes and GC to get back to the DataTable given only a TupleSlot
   */
  DataTable *data_table_;

  /**
   * Padding for flags or whatever we may want in the future. Determined by size of layout_version below. See
   * tuple_access_strategy.h for more details on Block header layout.
   */
  uint16_t padding_;

  /**
   * Layout version.
   */
  layout_version_t layout_version_;
  /**
   * The insert head tells us where the next insertion should take place. Notice that this counter is never
   * decreased as slot recycling does not happen on the fly with insertions. A background compaction process
   * scans through blocks and free up slots.
   * Since the block size is less then (1<<20) the uppper 12 bits of insert_head_ is free. We use the first bit (1<<31)
   * to indicate if the block is insertable.
   * If the first bit is 0, the block is insertable, otherwise one txn is inserting to this block
   */
  std::atomic<uint32_t> insert_head_;
  /**
   * Access controller of this block that coordinates access among Arrow readers, transactional workers
   * and the transformation thread. In practice this can be used almost like a lock.
   */
  BlockAccessController controller_;

  /**
   * Contents of the raw block.
   */
  byte content_[common::Constants::BLOCK_SIZE - sizeof(uintptr_t) - sizeof(uint16_t) - sizeof(layout_version_t) -
                sizeof(uint32_t) - sizeof(BlockAccessController)];
  // A Block needs to always be aligned to 1 MB, so we can get free bytes to
  // store offsets within a block in one 8-byte word

  /**
   * Get the offset of this block. Because the first bit insert_head_ is used to indicate the status
   * of the block, we need to clear the status bit to get the real offset
   * @return the offset which tells us where the next insertion should take place
   */
  uint32_t GetInsertHead() { return INT32_MAX & insert_head_.load(); }
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
    NOISEPAGE_ASSERT(!((static_cast<uintptr_t>(common::Constants::BLOCK_SIZE) - 1) & ((uintptr_t)block)),
                     "Address must be aligned to block size (last bits zero).");
    NOISEPAGE_ASSERT(offset < common::Constants::BLOCK_SIZE,
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
 * Used by SqlTable to map between col_oids in Schema and useful necessary information.
 */
using ColumnMap = std::unordered_map<catalog::col_oid_t, col_id_t>;
/**
 * Used by execution and storage layers to map between col_oids and offsets within a ProjectedRow
 */
using ProjectionMap = std::unordered_map<catalog::col_oid_t, uint16_t>;

/**
 * Denote whether a record modifies the logical delete column, used when DataTable inspects deltas
 */
enum class DeltaRecordType : uint8_t { UPDATE = 0, INSERT, DELETE };

/**
 * Types of LogRecords
 */
enum class LogRecordType : uint8_t { REDO = 1, DELETE, COMMIT, ABORT };

}  // namespace noisepage::storage

namespace std {
/**
 * Implements std::hash for TupleSlot.
 */
template <>
struct hash<noisepage::storage::TupleSlot> {
  /**
   * Returns the hash of the slot's contents.
   * @param slot the slot to be hashed.
   * @return the hash of the slot.
   */
  size_t operator()(const noisepage::storage::TupleSlot &slot) const { return hash<uintptr_t>()(slot.bytes_); }
};
}  // namespace std
