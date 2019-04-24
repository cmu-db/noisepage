#pragma once

#include <algorithm>
#include <functional>
#include <ostream>
#include <string_view>  // NOLINT
#include <unordered_map>
#include <utility>
#include <vector>
#include "catalog/catalog_defs.h"
#include "common/constants.h"
#include "common/container/bitmap.h"
#include "common/hash_util.h"
#include "common/macros.h"
#include "common/object_pool.h"
#include "common/strong_typedef.h"
#include "storage/block_access_controller.h"

namespace terrier::storage {
// Write Ahead Logging:
#define LOGGING_DISABLED nullptr
#define ACTION_FRAMEWORK_DISABLED nullptr

// All tuples potentially visible to txns should have a non-null attribute of version vector.
// This is not to be confused with a non-null version vector that has value nullptr (0).
#define VERSION_POINTER_COLUMN_ID ::terrier::storage::col_id_t(0)
#define NUM_RESERVED_COLUMNS 1u

// In type_util.h there are a total of 5 possible inlined attribute sizes:
// 1, 2, 4, 8, and 16-bytes (16 byte is the structure portion of varlen).
// Since we pack these attributes in descending size order, we can infer a
// columns size by tracking the locations of the attribute size boundaries.
// Therefore, we only need to track 4 locations because the exterior bounds
// are implicit.
#define NUM_ATTR_BOUNDARIES 4

STRONG_TYPEDEF(col_id_t, uint16_t);
STRONG_TYPEDEF(layout_version_t, uint32_t);

/**
 * A block is a chunk of memory used for storage. It does not have any meaning
 * unless interpreted by a TupleAccessStrategy. The header layout is documented in the class as well.
 * @see TupleAccessStrategy
 *
 * @warning If you change the layout please also change the way header sizes are computed in block layout!
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
   * Access controller of this block that coordinates access among Arrow readers, transactional workers
   * and the transformation thread. In practice this can be used almost like a lock.
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
  /**
   * Constructs a new varlen entry. The varlen entry will take ownership of the pointer given if reclaimable is true,
   * which means GC can delete the buffer pointed to when this entry is no longer visible in the storage engine.
   * @param content pointer to the varlen content itself
   * @param size length of the varlen content, in bytes (no C-style nul-terminator)
   * @param reclaim whether the varlen entry's content pointer can be deleted by itself. If the pointer was not
   *                    allocated by itself (e.g. inlined, or part of a dictionary batch or arrow buffer), it cannot
   *                    be freed by the GC, which simply calls delete.
   * @return constructed VarlenEntry object
   */
  static VarlenEntry Create(byte *content, uint32_t size, bool reclaim) {
    VarlenEntry result;
    TERRIER_ASSERT(size > InlineThreshold(), "small varlen values should be inlined");
    result.size_ = reclaim ? size : (INT32_MIN | size);  // the first bit denotes whether we can reclaim it
    std::memcpy(result.prefix_, content, sizeof(uint32_t));
    result.content_ = content;
    return result;
  }

  /**
   * Constructs a new varlen entry, with the associated varlen value inlined within the struct itself. This is only
   * possible when the inlined value is smaller than InlineThreshold() as defined. The value is copied and the given
   * pointer can be safely deallocated regardless of the state of the system.
   * @param content pointer to the varlen content
   * @param size length of the varlen content, in bytes (no C-style nul-terminator. Must be smaller than
   *             InlineThreshold())
   * @return constructed VarlenEntry object
   */
  static VarlenEntry CreateInline(const byte *content, uint32_t size) {
    TERRIER_ASSERT(size <= InlineThreshold(), "varlen value must be small enough for inlining to happen");
    VarlenEntry result;
    result.size_ = size;
    // overwrite the content field's 8 bytes for inline storage
    std::memcpy(result.prefix_, content, size);
    return result;
  }

  /**
   * @return The maximum size of the varlen field, in bytes, that can be inlined within the object. Any objects that are
   * larger need to be stored as a pointer to a separate buffer.
   */
  static constexpr uint32_t InlineThreshold() { return sizeof(VarlenEntry) - sizeof(uint32_t); }

  /**
   * @return length of the prefix of the varlen stored in the object for execution engine, if the varlen entry is not
   * inlined.
   */
  static constexpr uint32_t PrefixSize() { return sizeof(uint32_t); }

  /**
   * @return size of the varlen value stored in this entry, in bytes.
   */
  uint32_t Size() const { return static_cast<uint32_t>(INT32_MAX & size_); }

  /**
   * @return whether the content is inlined or not.
   */
  bool IsInlined() const { return Size() <= InlineThreshold(); }

  /**
   * Helper method to decide if the content needs to be GCed separately
   * @return whether the content can be deallocated by itself
   */
  bool NeedReclaim() const {
    // force a signed comparison, if our sign bit is set size_ is negative so the test returns false
    return size_ > static_cast<int32_t>(InlineThreshold());
  }

  /**
   * @return pointer to the stored prefix of the varlen entry
   */
  const byte *Prefix() const { return prefix_; }

  /**
   * @return pointer to the varlen entry contents.
   */
  const byte *Content() const { return IsInlined() ? prefix_ : content_; }

  /**
   * @return zero-copy view of the VarlenEntry as an immutable string that allows use with convenient STL functions
   * @warning It is the programmer's responsibility to ensure that std::string_view does not outlive the VarlenEntry
   */
  std::string_view StringView() const {
    return std::string_view(reinterpret_cast<const char *const>(Content()), Size());
  }

 private:
  int32_t size_;                   // buffer reclaimable => sign bit is 0 or size <= InlineThreshold
  byte prefix_[sizeof(uint32_t)];  // Explicit padding so that we can use these bits for inlined values or prefix
  const byte *content_;            // pointer to content of the varlen entry if not inlined
};
// To make sure our explicit padding is not screwing up the layout
static_assert(sizeof(VarlenEntry) == 16, "size of the class should be 16 bytes");

/**
 * Equality checker that checks the underlying varlen bytes are equal (deep)
 */
struct VarlenContentDeepEqual {
  /**
   *
   * @param lhs left hand side of comparison
   * @param rhs right hand side of comparison
   * @return whether the two varlen entries hold the same underlying value
   */
  bool operator()(const VarlenEntry &lhs, const VarlenEntry &rhs) const {
    if (lhs.Size() != rhs.Size()) return false;
    // TODO(Tianyu): Can optimize using prefixes
    return std::memcmp(lhs.Content(), rhs.Content(), lhs.Size()) == 0;
  }
};

/**
 * Hasher that hashes the entry using the underlying varlen value
 */
struct VarlenContentHasher {
  /**
   * @param obj object to hash
   * @return hash code of object
   */
  size_t operator()(const VarlenEntry &obj) const { return common::HashUtil::HashBytes(obj.Content(), obj.Size()); }
};

/**
 * Lexicographic comparison of two varlen entries.
 */
struct VarlenContentCompare {
  /**
   *
   * @param lhs left hand side of comparison
   * @param rhs right hand side of comparison
   * @return whether lhs < rhs in lexicographic order
   */
  bool operator()(const VarlenEntry &lhs, const VarlenEntry &rhs) const {
    // Compare up to the minimum of the two sizes
    int res = std::memcmp(lhs.Content(), rhs.Content(), std::min(lhs.Size(), rhs.Size()));
    if (res == 0) {
      // Shorter wins. If the two are equal, also return false.
      return lhs.Size() < rhs.Size();
    }
    return res < 0;
  }
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
