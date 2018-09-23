#pragma once

#include <algorithm>
#include <functional>
#include <ostream>
#include <utility>
#include <vector>
#include "common/constants.h"
#include "common/container/bitmap.h"
#include "common/macros.h"
#include "common/object_pool.h"
#include "common/typedefs.h"

namespace terrier::storage {
// Logging:
#define LOGGING_DISABLED nullptr

// We will always designate column to denote "presence" of a tuple, so that its null bitmap will effectively
// be the presence bit for tuples in this block. (i.e. a tuple is not considered valid with this column set to null,
// and thus blocks are free to handout the slot.) Generally this will just be the version vector.
// This is primarily to be used in the TAS layer
#define PRESENCE_COLUMN_ID col_id_t(0)

// All tuples potentially visible to txns should have a non-null attribute of version vector.
// This is not to be confused with a non-null version vector that has value nullptr (0).
// This is primarily to be used in the DataTable layer, even though it's the same as defined above.
#define VERSION_POINTER_COLUMN_ID PRESENCE_COLUMN_ID
#define LOGICAL_DELETE_COLUMN_ID col_id_t(1)
#define NUM_RESERVED_COLUMNS 2
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

/**
 * Stores metadata about the layout of a block.
 */
struct BlockLayout {
  /**
   * Constructs a new block layout.
   * @warning The resulting column ids WILL be reordered and are not the same as the indexes given in the
   * attr_sizes, as the constructor applies optimizations based on sizes. It is up to the caller to then
   * associate these "column ids" with the right upper level concepts.
   *
   * @param attr_sizes vector of attribute sizes.
   */
  explicit BlockLayout(std::vector<uint8_t> attr_sizes)
      : attr_sizes_(std::move(attr_sizes)),
        tuple_size_(ComputeTupleSize()),
        header_size_(ComputeHeaderSize()),
        num_slots_(ComputeNumSlots()) {
    TERRIER_ASSERT(!attr_sizes_.empty() && static_cast<uint16_t>(attr_sizes_.size()) <= common::Constants::MAX_COL,
                   "number of columns must be between 1 and 32767");
    TERRIER_ASSERT(num_slots_ != 0, "number of slots cannot be 0!");
    // sort the attributes when laying out memory to minimize impact of padding
    std::sort(attr_sizes_.begin(), attr_sizes_.end(), std::greater<>());
  }

  /**
   * Number of columns.
   */
  const uint16_t NumColumns() const { return static_cast<uint16_t>(attr_sizes_.size()); }

  /**
   * attribute size at given col_id.
   */
  uint8_t AttrSize(col_id_t col_id) const { return attr_sizes_.at(!col_id); }

  /**
   * Tuple size.
   */
  const uint32_t TupleSize() const { return tuple_size_; }

  /**
   * Header size.
   */
  const uint32_t HeaderSize() const { return header_size_; }

  /**
   * Number of slots in the tuple.
   */
  const uint32_t NumSlots() const { return num_slots_; }

 private:
  std::vector<uint8_t> attr_sizes_;
  // Cached values so that we don't have to iterate through attr_sizes_ every time.
  const uint32_t tuple_size_;
  const uint32_t header_size_;
  const uint32_t num_slots_;

 private:
  uint32_t ComputeTupleSize() const {
    uint32_t result = 0;
    for (auto size : attr_sizes_) result += size;
    return result;
  }

  uint32_t ComputeHeaderSize() const {
    return static_cast<uint32_t>(sizeof(uint32_t) * 3  // layout_version, num_records, num_slots
                                 + NumColumns() * sizeof(uint32_t) + sizeof(uint16_t) + NumColumns() * sizeof(uint8_t));
  }

  uint32_t ComputeNumSlots() const {
    // TODO(Tianyu):
    // subtracting 1 from this number so we will always have
    // space to pad each individual bitmap to full bytes (every attribute is
    // at least a byte). Subtracting another 1 to account for padding. Somebody can come and fix
    // this later, because I don't feel like thinking about this now.
    // TODO(Tianyu): Now with sortedness in our layout, we don't necessarily have the worse case where padding can take
    // up to the size of 1 tuple, so this can probably change to be more optimistic,
    return 8 * (common::Constants::BLOCK_SIZE - header_size_) / (8 * tuple_size_ + NumColumns()) - 2;
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
 * Denote whether a record modifies the logical delete column, used when DataTable inspects deltas
 * TODO(Matt): could be used by the GC for recycling
 */
enum class LogicalDeleteModificationType : uint8_t { NONE = 0, INSERT, DELETE };
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
