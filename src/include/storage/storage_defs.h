#pragma once
#include <sstream>
#include "common/common_defs.h"
#include "common/macros.h"
#include "common/object_pool.h"
#include "common/printable.h"

namespace terrier {
namespace storage {
// TODO(Tianyu): We probably want to align this to some level
/**
 * A block is a chunk of memory used for storage. It does not have any meaning
 * unless interpreted by a @see TupleAccessStrategy
 */
class RawBlock {
 public:
  RawBlock() {
    // Intentionally unused
    (void)content_;
  }
  byte content_[Constants::BLOCK_SIZE];
  // A Block needs to always be aligned to 1 MB, so we can get free bytes to
  // store offsets within a block in ine 8-byte word.
} __attribute__((aligned(Constants::BLOCK_SIZE)));

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
  TupleSlot(RawBlock *block, uint32_t offset) : bytes_((uintptr_t)block | offset) {
    // Assert that the address is aligned up to block size (i.e. last bits zero)
    PELOTON_ASSERT(!((static_cast<uintptr_t>(Constants::BLOCK_SIZE) - 1) & ((uintptr_t)block)));
    // Assert that the offset is smaller than the block size, so we can fit
    // it in the 0 bits at the end of the address
    PELOTON_ASSERT(offset < Constants::BLOCK_SIZE);
  }

  /**
   * @return ptr to the head of the block
   */
  RawBlock *GetBlock() const {
    // Get the first 11 bytes as the ptr
    return reinterpret_cast<RawBlock *>(bytes_ & ~(static_cast<uintptr_t>(Constants::BLOCK_SIZE) - 1));
  }

  /**
   * @return offset of the tuple within a block.
   */
  uint32_t GetOffset() const {
    return static_cast<uint32_t>(bytes_ & (static_cast<uintptr_t>(Constants::BLOCK_SIZE) - 1));
  }

  bool operator==(const TupleSlot &other) const { return bytes_ == other.bytes_; }

  bool operator!=(const TupleSlot &other) const { return bytes_ != other.bytes_; }

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
 * A block store is essentially an object pool. However, all blocks should be
 * aligned, so we will need to use the default constructor instead of raw
 * malloc.
 */
using BlockStore = ObjectPool<RawBlock, DefaultConstructorAllocator<RawBlock>>;
}  // namespace storage
}  // namespace terrier

namespace std {
template <>
struct hash<terrier::storage::TupleSlot> {
  size_t operator()(const terrier::storage::TupleSlot &slot) const { return hash<uintptr_t>()(slot.bytes_); }
};
}  // namespace std