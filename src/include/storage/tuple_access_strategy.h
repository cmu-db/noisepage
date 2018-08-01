#pragma once

#include <utility>
#include <vector>
#include "common/concurrent_bitmap.h"
#include "common/macros.h"
#include "storage/storage_defs.h"

// We will always designate column to denote "presence" of a tuple, so that its null bitmap will effectively
// be the presence bit for tuples in this block. (i.e. a tuple is not considered valid with this column set to null,
// and thus blocks are free to handout the slot.) Generally this will just be the version vector.
#define PRESENCE_COLUMN_ID 0

namespace terrier {
namespace storage {

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
void InitializeRawBlock(RawBlock *raw, const BlockLayout &layout, layout_version_t layout_version);

/**
 * Code for accessing data within a block. This code is eventually compiled and
 * should be stateless, so no fields other than const BlockLayout.
 */
class TupleAccessStrategy {
 private:
  // TODO(Tianyu): These two classes should be aligned for LLVM
  /**
   * A mini block stores individual columns. Mini block layout:
   * ----------------------------------------------------
   * | null-bitmap (pad up to byte) | val1 | val2 | ... |
   * ----------------------------------------------------
   * Warning, 0 means null
   */
  struct MiniBlock {
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
  struct Block {
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
    uint32_t &NumSlots() { return *reinterpret_cast<uint32_t *>(block_.content_); }

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

    RawBlock block_;
  };

 public:
  /**
   * Initializes a TupleAccessStrategy
   * @param layout block layout to use
   */
  explicit TupleAccessStrategy(BlockLayout layout) : layout_(std::move(layout)) {}

  // InitializeRawBlock requires knowledge about an interpreted Block.
  friend void InitializeRawBlock(RawBlock *raw, const BlockLayout &layout, layout_version_t layout_version);

  /* Vectorized Access */
  /**
   * @param block block to access
   * @param col offset representing the column
   * @return pointer to the bitmap of the specified column on the given block
   */
  common::RawConcurrentBitmap *ColumnNullBitmap(RawBlock *block, uint16_t col) const {
    return reinterpret_cast<Block *>(block)->Column(col)->NullBitmap();
  }

  /**
   * @param block block  to access
   * @param col offset representing the column
   * @return pointer to the start of the column
   */
  byte *ColumnStart(RawBlock *block, uint16_t col) const {
    return reinterpret_cast<Block *>(block)->Column(col)->ColumnStart(layout_);
  }

  /* Tuple-level access */
  /**
   * @param slot tuple slot to access
   * @param col offset representing the column
   * @return a pointer to the attribute, or nullptr if attribute is null.
   */
  byte *AccessWithNullCheck(const TupleSlot slot, uint16_t col) const {
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
    if (ColumnNullBitmap(slot.GetBlock(), col)->Flip(slot.GetOffset(), true)  // Noop if already null
        && col == PRESENCE_COLUMN_ID)
      slot.GetBlock()->num_records_--;
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
    common::RawConcurrentBitmap *bitmap = ColumnNullBitmap(block, PRESENCE_COLUMN_ID);
    for (uint32_t i = 0; i < layout_.num_slots_; i++) {
      if (bitmap->Flip(i, false)) {
        slot = TupleSlot(block, i);
        block->num_records_++;
        return true;
      }
    }
    return false;
  }

  const BlockLayout &GetBlockLayout() const { return layout_; }

 private:
  // TODO(Tianyu): This will be baked in for codegen, not a field.
  const BlockLayout layout_;
};
}  // namespace storage
}  // namespace terrier
