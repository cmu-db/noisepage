#pragma once

#include <utility>
#include <vector>

#include "common/container/concurrent_bitmap.h"
#include "common/macros.h"
#include "storage/arrow_block_metadata.h"
#include "storage/storage_defs.h"
#include "storage/storage_util.h"

namespace noisepage::storage {

class DataTable;

/**
 * Code for accessing data within a block. This code is eventually compiled and
 * should be stateless, so no fields other than const BlockLayout.
 */
class TupleAccessStrategy {
 private:
  /*
   * A mini block stores individual columns. Mini block layout:
   * ----------------------------------------------------
   * | null-bitmap (pad up to 8 bytes) | val1 | val2 | ... |
   * ----------------------------------------------------
   * Warning, 0 means null
   */
  struct MiniBlock {
    MEM_REINTERPRETATION_ONLY(MiniBlock)
    // return a pointer to the start of the column. (use as an array)
    byte *ColumnStart(const BlockLayout &layout, const col_id_t col_id) {
      return StorageUtil::AlignedPtr(sizeof(uint64_t),  // always padded up to 8 bytes
                                     varlen_contents_ + common::RawBitmap::SizeInBytes(layout.NumSlots()));
    }

    // return The null-bitmap of this column
    common::RawConcurrentBitmap *NullBitmap() {
      return reinterpret_cast<common::RawConcurrentBitmap *>(varlen_contents_);
    }

    byte varlen_contents_[0];
  };

  /*
   * Block Header layout:
   * -----------------------------------------------------------------------------------------------------------------
   * | data_table *(64) | padding (16) | layout_version (16) | insert_head (32) |        control_block (64)          |
   * -----------------------------------------------------------------------------------------------------------------
   * | ArrowBlockMetadata | attr_offsets[num_col] (32) | bitmap for slots (64-bit aligned) | data (64-bit aligned)   |
   * -----------------------------------------------------------------------------------------------------------------
   *
   * Note that we will never need to span a tuple across multiple pages if we enforce
   * block size to be 1 MB and columns to be less than MAX_COL
   */
  struct Block {
    MEM_REINTERPRETATION_ONLY(Block)

    // TODO(Tianyu): Is header access going to be too slow? If so, consider storing offsets to jump to within headers as
    // well.
    ArrowBlockMetadata &GetArrowBlockMetadata() { return *reinterpret_cast<ArrowBlockMetadata *>(block_.content_); }

    // return reference to attr_offsets. Use as an array.
    uint32_t *AttrOffsets(const BlockLayout &layout) {
      return reinterpret_cast<uint32_t *>(block_.content_ + ArrowBlockMetadata::Size(layout.NumColumns()));
    }

    // return reference to the bitmap for slots. Use as a member
    common::RawConcurrentBitmap *SlotAllocationBitmap(const BlockLayout &layout) {
      return reinterpret_cast<common::RawConcurrentBitmap *>(
          StorageUtil::AlignedPtr(sizeof(uint64_t), AttrOffsets(layout) + layout.NumColumns()));
    }

    // return the miniblock for the column at the given offset.
    MiniBlock *Column(const BlockLayout &layout, const col_id_t col_id) {
      byte *head = reinterpret_cast<byte *>(this) + AttrOffsets(layout)[col_id.UnderlyingValue()];
      return reinterpret_cast<MiniBlock *>(head);
    }

    RawBlock block_;
  };

 public:
  /**
   * Initializes a TupleAccessStrategy
   * @param layout block layout to use
   */
  explicit TupleAccessStrategy(BlockLayout layout);

  /**
   * Initializes a new block to conform to the layout given. This will write the
   * headers and divide up the blocks into mini blocks(each mini block contains
   * a column). The raw block needs to be 0-initialized (by default when given out
   * from a block store), otherwise it will cause undefined behavior.
   *
   * @param data_table pointer to the DataTable to reference from this block
   * @param raw pointer to the raw block to initialize
   * @param layout_version the layout version of this block
   */
  void InitializeRawBlock(storage::DataTable *data_table, RawBlock *raw, layout_version_t layout_version) const;

  /**
   * @param block block to access
   * @return the ArrowBlockMetadata object of the requested block
   */
  ArrowBlockMetadata &GetArrowBlockMetadata(RawBlock *block) const {
    return reinterpret_cast<Block *>(block)->GetArrowBlockMetadata();
  }

  /**
   * @param slot tuple slot value to check
   * @return whether the given slot is occupied by a tuple
   */
  bool Allocated(const TupleSlot slot) const {
    return reinterpret_cast<Block *>(slot.GetBlock())->SlotAllocationBitmap(layout_)->Test(slot.GetOffset());
  }

  /**
   * @param block block to access
   * @param col_id id of the column
   * @return pointer to the bitmap of the specified column on the given block
   */
  common::RawConcurrentBitmap *ColumnNullBitmap(RawBlock *block, const col_id_t col_id) const {
    NOISEPAGE_ASSERT((col_id.UnderlyingValue()) < layout_.NumColumns(), "Column out of bounds!");
    return reinterpret_cast<Block *>(block)->Column(layout_, col_id)->NullBitmap();
  }

  /**
   * @param block block to access
   * @param col_id id of the column
   * @return pointer to the start of the column
   */
  byte *ColumnStart(RawBlock *block, const col_id_t col_id) const {
    NOISEPAGE_ASSERT((col_id.UnderlyingValue()) < layout_.NumColumns(), "Column out of bounds!");
    return reinterpret_cast<Block *>(block)->Column(layout_, col_id)->ColumnStart(layout_, col_id);
  }

  /**
   * @param slot tuple slot to access
   * @param col_id id of the column
   * @return a pointer to the attribute, or nullptr if attribute is null.
   */
  byte *AccessWithNullCheck(const TupleSlot slot, const col_id_t col_id) const {
    NOISEPAGE_ASSERT(slot.GetOffset() < layout_.NumSlots(), "Offset out of bounds!");
    if (!ColumnNullBitmap(slot.GetBlock(), col_id)->Test(slot.GetOffset())) return nullptr;
    return ColumnStart(slot.GetBlock(), col_id) + layout_.AttrSize(col_id) * slot.GetOffset();
  }

  /**
   * Returns a pointer to the attribute, ignoring the presence bit.
   * @param slot tuple slot to access
   * @param col_id id of the column
   * @return a pointer to the attribute
   */
  byte *AccessWithoutNullCheck(const TupleSlot slot, const col_id_t col_id) const {
    NOISEPAGE_ASSERT(slot.GetOffset() < layout_.NumSlots(), "Offset out of bounds!");
    return ColumnStart(slot.GetBlock(), col_id) + layout_.AttrSize(col_id) * slot.GetOffset();
  }

  /**
   * Returns a pointer to the attribute. If the attribute is null, set null to
   * false.
   * @param slot tuple slot to access
   * @param col_id id of the column
   * @return a pointer to the attribute.
   */
  byte *AccessForceNotNull(const TupleSlot slot, const col_id_t col_id) const {
    NOISEPAGE_ASSERT(slot.GetOffset() < layout_.NumSlots(), "Offset out of bounds!");
    common::RawConcurrentBitmap *bitmap = ColumnNullBitmap(slot.GetBlock(), col_id);
    if (!bitmap->Test(slot.GetOffset())) bitmap->Flip(slot.GetOffset(), false);
    return ColumnStart(slot.GetBlock(), col_id) + layout_.AttrSize(col_id) * slot.GetOffset();
  }

  /**
   * Get an attribute's null value
   * @param slot tuple slot to access
   * @param col_id id of the column
   * @return true if null, false otherwise
   */
  bool IsNull(const TupleSlot slot, const col_id_t col_id) const {
    NOISEPAGE_ASSERT(slot.GetOffset() < layout_.NumSlots(), "Offset out of bounds!");
    return !ColumnNullBitmap(slot.GetBlock(), col_id)->Test(slot.GetOffset());
  }

  /**
   * Set an attribute null.
   * @param slot tuple slot to access
   * @param col_id id of the column
   */
  void SetNull(const TupleSlot slot, const col_id_t col_id) const {
    NOISEPAGE_ASSERT(slot.GetOffset() < layout_.NumSlots(), "Offset out of bounds!");
    ColumnNullBitmap(slot.GetBlock(), col_id)->Flip(slot.GetOffset(), true);
  }

  /**
   * Set an attribute not null.
   * @param slot tuple slot to access
   * @param col_id id of the column
   */
  void SetNotNull(const TupleSlot slot, const col_id_t col_id) const {
    NOISEPAGE_ASSERT(slot.GetOffset() < layout_.NumSlots(), "Offset out of bounds!");
    ColumnNullBitmap(slot.GetBlock(), col_id)->Flip(slot.GetOffset(), false);
  }

  /**
   * Flip a deallocated slot to be allocated again. This is useful when compacting a block,
   * as we want to make decisions in the compactor on what slot to use, not in this class.
   * This method should not be called other than that.
   * @param slot the tuple slot to reallocate. Must be currently deallocated.
   */
  void Reallocate(TupleSlot slot) const {
    NOISEPAGE_ASSERT(!Allocated(slot), "Can only reallocate slots that are deallocated");
    reinterpret_cast<Block *>(slot.GetBlock())->SlotAllocationBitmap(layout_)->Flip(slot.GetOffset(), false);
  }

  /**
   * Allocates a slot for a new tuple, writing to the given reference.
   * @param block block to allocate a slot in.
   * @param[out] slot tuple to write to.
   * @return true if the allocation succeeded, false if no space could be found.
   */
  bool Allocate(RawBlock *block, TupleSlot *slot) const;

  /**
   * @param block the block to access
   * @return pointer to the allocation bitmap of the block
   */
  common::RawConcurrentBitmap *AllocationBitmap(RawBlock *block) const {
    return reinterpret_cast<Block *>(block)->SlotAllocationBitmap(layout_);
  }

  /**
   * Deallocates a slot.
   * @param slot the slot to free up
   */
  void Deallocate(const TupleSlot slot) const {
    NOISEPAGE_ASSERT(Allocated(slot), "Can only deallocate slots that are allocated");
    reinterpret_cast<Block *>(slot.GetBlock())->SlotAllocationBitmap(layout_)->Flip(slot.GetOffset(), true);
    // TODO(Tianyu): Make explicit that this operation does not reset the insertion head, and the block
    // is still considered "full" and will not be inserted into.
  }

  /**
   * Returns the block layout.
   * @return the block layout.
   */
  const BlockLayout &GetBlockLayout() const { return layout_; }

  /**
   * Compare and swap block status to busy. Expect that block to be idle, if yes, set the block status to be busy
   * @param block the block to compare and set
   * @return true if the set operation succeeded, false if the block is already busy
   */
  bool SetBlockBusyStatus(RawBlock *block) const {
    uint32_t old_val = ClearBit(block->insert_head_.load());
    return block->insert_head_.compare_exchange_strong(old_val, SetBit(old_val));
  }

  /**
   * Compare and swap block status to idle. Expect that block to be busy, if yes, set the block status to be idle
   * @param block the block to compare and set
   * @return true if the set operation succeeded, false if the block is already idle
   */
  bool ClearBlockBusyStatus(RawBlock *block) const {
    uint32_t val = block->insert_head_.load();
    NOISEPAGE_ASSERT(val == SetBit(val), "The busy bit should be set ");
    return block->insert_head_.compare_exchange_strong(val, ClearBit(val));
  }

  /**
   * Set the first bit of given val to 0, helper function used by ClearBlockBusyStatus
   * @param val the value to be set
   * @return the changed value with first bit set to 0
   */
  static uint32_t ClearBit(uint32_t val) { return val & INT32_MAX; }

  /**
   * Set the first bit of given val to 1, helper function used by SetBlockBusyStatus
   * @param val val the value to be set
   * @return the changed value with first bit set to 1
   */
  static uint32_t SetBit(uint32_t val) { return val | INT32_MIN; }

 private:
  const BlockLayout layout_;
  // Start of each mini block, in offset to the start of the block
  std::vector<uint32_t> column_offsets_;
};
}  // namespace noisepage::storage
