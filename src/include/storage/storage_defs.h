#pragma once
#include <sstream>
#include "common/constants.h"
#include "common/json_serializable.h"
#include "common/macros.h"
#include "common/object_pool.h"
#include "common/typedefs.h"

namespace terrier::storage {
// TODO(Tianyu): This code eventually should be compiled, which would eliminate
// BlockLayout as a runtime object, instead baking them in as compiled code
// (Think of this as writing the class with a BlockLayout template arg, except
// template instantiation is done by LLVM at runtime and not at compile time.
struct BlockLayout {
  BlockLayout(uint16_t num_attrs, std::vector<uint8_t> attr_sizes)
      : num_cols_(num_attrs),
        attr_sizes_(std::move(attr_sizes)),
        tuple_size_(ComputeTupleSize()),
        header_size_(HeaderSize()),
        num_slots_(NumSlots()) {}

  const uint16_t num_cols_;
  const std::vector<uint8_t> attr_sizes_;
  // Cached so we don't have to iterate through attr_sizes every time
  const uint32_t tuple_size_;
  const uint32_t header_size_;
  const uint32_t num_slots_;

 private:
  uint32_t ComputeTupleSize() {
    PELOTON_ASSERT(num_cols_ == attr_sizes_.size());
    uint32_t result = 0;
    for (auto size : attr_sizes_) result += size;
    return result;
  }

  uint32_t HeaderSize() {
    return sizeof(uint32_t) * 3  // layout_version, num_records, num_slots
           + num_cols_ * sizeof(uint32_t) + sizeof(uint16_t) + num_cols_ * sizeof(uint8_t);
  }

  uint32_t NumSlots() {
    // Need to account for extra bitmap structures needed for each attribute.
    // TODO(Tianyu): I am subtracting 1 from this number so we will always have
    // space to pad each individual bitmap to full bytes (every attribute is
    // at least a byte). Somebody can come and fix this later, because I don't
    // feel like thinking about this now.
    return 8 * (Constants::BLOCK_SIZE - header_size_) / (8 * tuple_size_ + num_cols_) - 1;
  }
};

/**
 * A block is a chunk of memory used for storage. It does not have any meaning
 * unless interpreted by a @see TupleAccessStrategy
 */
struct RawBlock {
  layout_version_t layout_version_;
  uint32_t num_records_;
  byte content_[Constants::BLOCK_SIZE - 2 * sizeof(uint32_t)];
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

class ProjectedRow {
 public:
  ProjectedRow() = delete;
  DISALLOW_COPY_AND_MOVE(ProjectedRow)
  ~ProjectedRow() = delete;

  uint16_t NumColumns() const { return num_cols_; }

  uint16_t *ColumnIds() { return nullptr; }

  const uint16_t *ColumnIds() const { return nullptr; }

  byte *AccessForceNotNull(uint16_t offset) { return nullptr; }

  void SetNull(uint16_t offset) { (void)varlen_contents_; }

  byte *AttrWithNullCheck(uint16_t offset) { return nullptr; }

  const byte *AccessWithNullCheck(uint16_t offset) const { return nullptr; }

 private:
  const uint16_t num_cols_;
  byte varlen_contents_[0];
};

struct DeltaRecord {
  DeltaRecord *next_;
  timestamp_t timestamp_;
  ProjectedRow delta_;
};
}  // namespace terrier::storage

namespace std {
template <>
struct hash<terrier::storage::TupleSlot> {
  size_t operator()(const terrier::storage::TupleSlot &slot) const { return hash<uintptr_t>()(slot.bytes_); }
};
}  // namespace std