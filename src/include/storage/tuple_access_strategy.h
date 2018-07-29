#pragma once

#include <vector>
#include "common/concurrent_bitmap.h"
#include "common/macros.h"
#include "storage/storage_defs.h"

// We will always layout the primary key column (or a column that is part of the
// primary key if multi-column), so that its nullmap will effectively be the
// presence bit for tuples in this block.
#define PRIMARY_KEY_OFFSET 0
namespace terrier {
namespace storage {


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

  // header size is smaller than block size which is 1MB, so this cast is safe
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
    return 8 * (Constants::BLOCK_SIZE - header_size_) / (8 * tuple_size_ + num_cols_) - 1;
  }
};

/**
 * Initializes a new block to conform to the layout given. This will write the
 * headers and divide up the blocks into mini blocks(each mini block contains
 * a column). The raw block needs to be 0-initialized (by default when given out
 * from a block store), otherwise it will cause undefined behavior.
 *
 * @param raw pointer to the raw block to initialize
 * @param layout block layout to use (can be compiled)
 * @param layout_version the layout version of this block
 */
void InitializeRawBlock(RawBlock *raw, const BlockLayout &layout, uint32_t layout_version);
namespace {
// TODO(Tianyu): These two classes should be aligned for LLVM
/**
 * A mini block stores individual columns. Mini block layout:
 * ----------------------------------------------------
 * | null-bitmap (pad up to byte) | val1 | val2 | ... |
 * ----------------------------------------------------
 * Warning, 0 means null
 */
struct MiniBlock {
 public:
  /**
   * A mini-block is always reinterpreted from a raw piece of memory
   * and should never be initialized, copied, moved, or on the stack.
   */
  MiniBlock() = delete;
  DISALLOW_COPY_AND_MOVE(MiniBlock);
  ~MiniBlock() = delete;
  /**
   * @param layout the layout of this block
   * @return a pointer to the start of the column. (use as an array)
   */
  byte *ColumnStart(const BlockLayout &layout) { return varlen_contents_ + common::BitmapSize(layout.num_slots_); }

  /**
   * @return The null-bitmap of this column
   */
  common::RawConcurrentBitmap *NullBitmap() {
    return reinterpret_cast<common::RawConcurrentBitmap *>(varlen_contents_);
  }

  // Because where the other fields start will depend on the specific layout,
  // reinterpreting the rest as bytes is the best we can do without LLVM.
  byte varlen_contents_[0]{};
};

/**
 * Block Header layout:
 * TODO(Tianyu): Maybe move the first 3 fields to RawBlock
 * --------------------------------------------------------------------------
 * | layout_version | num_records | num_slots | attr_offsets[num_attributes] | // 32-bit fields
 * --------------------------------------------------------------------------
 * | num_attrs (16-bit) | attr_sizes[num_attr] (8-bit) |   ...content        |
 * --------------------------------------------------------------------------
 *
 * This is laid out in this order, because except for num_records,
 * the other fields are going to be immutable for a block's lifetime,
 * and except for block id, all the other fields are going to be baked in to
 * the code and never read. Laying out in this order allows us to only load the
 * first 64 bits we care about in the header in compiled code.
 *
 * Note that we will never need to span a tuple across multiple pages if we enforce
 * block size to be 1 MB and columns to be less than 65535 (max uint16_t)
 */
// This is packed because these will only be constructed from compact
// storage bytes, not on the fly. Layout optimization should be left
// for LLVM later.
struct PACKED Block {
  /**
   * A block is always reinterpreted from a raw piece of memory
   * and should never be initialized, copied, moved, or on the stack.
   */
  Block() = delete;
  DISALLOW_COPY_AND_MOVE(Block);
  ~Block() = delete;

  /**
   * @param offset offset representing the column
   * @return the miniblock for the column at the given offset.
   */
  MiniBlock *Column(uint16_t offset) {
    byte *head = reinterpret_cast<byte *>(this) + AttrOffets()[offset];
    return reinterpret_cast<MiniBlock *>(head);
  }

  /**
   * @return reference to num_slots. Use as a member.
   */
  uint32_t &NumSlots() { return *reinterpret_cast<uint32_t *>(varlen_contents_); }

  /**
   * @return reference to attr_offsets. Use as an array.
   */
  uint32_t *AttrOffets() { return &NumSlots() + 1; }

  /**
   * @param layout layout of the block
   * @return reference to num_attrs. Use as a member.
   */
  uint16_t &NumAttrs(const BlockLayout &layout) {
    return *reinterpret_cast<uint16_t *>(AttrOffets() + layout.num_cols_);
  }

  /**
   * @param layout layout of the block
   * @return reference to attr_sizes. Use as an array.
   */
  uint8_t *AttrSizes(const BlockLayout &layout) { return reinterpret_cast<uint8_t *>(&NumAttrs(layout) + 1); }
  // Because where the other fields start will depend on the specific layout,
  // reinterpreting the rest as bytes is the best we can do without LLVM.
  byte varlen_contents_[0];
};
}  // namespace

/**
 * Code for accessing data within a block. This code is eventually compiled and
 * should be stateless, so no fields other than const BlockLayout.
 */
class TupleAccessStrategy {
 public:
  /**
   * Initializes a TupleAccessStrategy
   * @param layout block layout to use
   */
  explicit TupleAccessStrategy(BlockLayout layout) : layout_(std::move(layout)) {}

  /* Vectorized Access */
  /**
   * @param block block to access
   * @param col offset representing the column
   * @return pointer to the bitmap of the specified column on the given block
   */
  common::RawConcurrentBitmap *ColumnNullBitmap(RawBlock *block, uint16_t col) const {
    return reinterpret_cast<Block *>(block->content_)->Column(col)->NullBitmap();
  }

  /**
   * @param block block  to access
   * @param col offset representing the column
   * @return pointer to the start of the column
   */
  byte *ColumnStart(RawBlock *block, uint16_t col) const {
    return reinterpret_cast<Block *>(block->content_)->Column(col)->ColumnStart(layout_);
  }

  /* Tuple-level access */
  /**
   * @param slot tuple slot to access
   * @param col offset representing the column
   * @return a pointer to the attribute, or nullptr if attribute is null.
   */
  byte *AccessWithNullCheck(TupleSlot slot, uint16_t col) const {
    if (!ColumnNullBitmap(slot.GetBlock(), col)->Test(slot.GetOffset())) return nullptr;
    return ColumnStart(slot.GetBlock(), col) + layout_.attr_sizes_[col] * slot.GetOffset();
  }

  /**
   * Returns a pointer to the attribute. If the attribute is null, set null to
   * false.
   * @param slot tuple slot to access
   * @param col offset representing the column
   * @return a pointer to the attribute.
   */
  byte *AccessForceNotNull(TupleSlot slot, uint16_t col) const {
    // Noop if not null
    // TODO(Tianyu): Don't compare and swap this shit
    ColumnNullBitmap(slot.GetBlock(), col)->Flip(slot.GetOffset(), false);
    return ColumnStart(slot.GetBlock(), col) + layout_.attr_sizes_[col] * slot.GetOffset();
  }

  /**
   * Set an attribute null. If called on the primary key column (0), this is
   * considered freeing.
   * @param slot tuple slot to access
   * @param col offset representing the column
   */
  void SetNull(TupleSlot slot, uint16_t col) const {
    // Noop if already null
    ColumnNullBitmap(slot.GetBlock(), col)->Flip(slot.GetOffset(), true);
  }

  /* Allocation and Deallocation */
  /**
   * Allocates a slot for a new tuple, writing its offset to the given reference.
   * Returns false if there is no space in this block.
   * @param block block to allocate a tuple in
   * @param offset result
   * @return true if the allocation is successful, false if no space can be found.
   */
  bool Allocate(RawBlock *block, TupleSlot &slot) const {
    // TODO(Tianyu): Really inefficient for now. Again, embarrassingly
    // vectorizable. Optimize later.
    common::RawConcurrentBitmap *bitmap = ColumnNullBitmap(block, PRIMARY_KEY_OFFSET);
    for (uint32_t i = 0; i < layout_.num_slots_; i++) {
      if (bitmap->Flip(i, false)) {
        slot = TupleSlot(block, i);
        return true;
      }
    }
    return false;
  }

  const BlockLayout &GetBlockLayout() const {
    return layout_;
  }

 private:
  // TODO(Tianyu): This will be baked in for codegen, not a field.
  const BlockLayout layout_;
};
}  // namespace storage
}  // namespace terrier
