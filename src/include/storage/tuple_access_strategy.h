#pragma once

#include <vector>
#include "common/concurrent_bitmap.h"
#include "common/macros.h"
#include "storage/storage_defs.h"
#include "storage/block_store.h"

// We will always layout the primary key column (or a column that is part of the
// primary key if multi-column), so that its nulmap
#define PRIMARY_KEY_OFFSET 0
namespace terrier {
namespace storage {

// TODO(Tianyu): This code eventually should be compiled, which would eliminate
// BlockLayout as a runtime object, instead baking them in as compiled code
// (Think of this as writing the class with a BlockLayout template arg, except
// template instantiation is done by LLVM at runtime and not at compile time.
struct BlockLayout {
  BlockLayout(uint16_t num_attrs, std::vector<const uint8_t> attr_sizes)
      : num_attrs_(num_attrs),
        attr_sizes_(std::move(attr_sizes)),
        tuple_size_(ComputeTupleSize()),
        header_size_(HeaderSize()),
        num_slots_(NumSlots()) {}

  const uint16_t num_attrs_;
  const std::vector<const uint8_t> attr_sizes_;
  // Cached so we don't have to iterate through attr_sizes every time
  const uint32_t tuple_size_;
  const uint32_t header_size_;
  const uint32_t num_slots_;

 private:
  uint32_t ComputeTupleSize() {
    PELOTON_ASSERT(num_attrs_ == attr_sizes_.size());
    uint32_t result = 0;
    for (auto size : attr_sizes_)
      result += size;
    return result;
  }

  uint32_t HeaderSize() {
    return sizeof(uint32_t) * 3 // block_id, num_records, num_slots
        + num_attrs_ * sizeof(uint32_t)
        + sizeof(uint16_t)
        + num_attrs_ * sizeof(uint8_t);
  }

  uint32_t NumSlots() {
    // Need to account for extra bitmap structures needed for each attribute.
    // TODO(Tianyu): I am subtracting 1 from this number so we will always have
    // space to pad each individual bitmap to full bytes (every attribute is
    // at least a byte). Somebody can come and fix this later, because I don't
    // feel like thinking about this now.
    return 8 * (Constants::BLOCK_SIZE - header_size_)
        / (8 * tuple_size_ + num_attrs_) - 1;
  }

};

// raw should be zeroed out.
void InitializeRawBlock(RawBlock *raw,
                        const BlockLayout &layout,
                        block_id_t id);
namespace {
// TODO(Tianyu): These two classes should be aligned for LLVM
// For individual columns
// Mini block layout:
// ------------------------------------------------------------
// | null-bitmap (pad up to byte-aligned) | val1 | val2 | ... |
// ------------------------------------------------------------
// Warning, 0 means null
struct MiniBlock {
 public:
  MiniBlock() = delete;
  DISALLOW_COPY_AND_MOVE(MiniBlock);
  ~MiniBlock() = delete;
  byte *ColumnStart(const BlockLayout &layout) {
    return varlen_contents_ + BitmapSize(layout.num_slots_);
  }

  RawConcurrentBitmap *NullBitmap() {
    return reinterpret_cast<RawConcurrentBitmap *>(varlen_contents_);
  }

  // Because where the other fields start will depend on the specific layout,
  // reinterpreting the rest as bytes is the best we can do without LLVM.
  byte varlen_contents_[0]{};
};

// Block Header layout:
// TODO(Tianyu): Maybe move the first 3 fields to RawBlock
// ---------------------------------------------------------------------
// | block_id | num_records | num_slots | attr_offsets[num_attributes] | // 32-bit fields
// ---------------------------------------------------------------------
// | num_attrs (16-bit) | attr_sizes[num_attr] (8-bit) |   ...content  |
// ---------------------------------------------------------------------
//
//
// This is laid out in this order, because except for num_records,
// the other fields are going to be immutable for a block's lifetime,
// and except for block id, all the other fields are going to be baked in to
// the code and never read. Laying out in this order allows us to only load the
// first 64 bits we care about in the header in compiled code.
//
// Note that we will never need to span a tuple across multiple pages if we enforce
// block size to be 1 MB and columns to be less than 65535 (max uint16_t)
struct PACKED Block {
  // This is packed because these will only be constructed from compact
  // storage bytes, not on the fly.
  Block() = delete;
  DISALLOW_COPY_AND_MOVE(Block);
  ~Block() = delete;

  MiniBlock *Column(uint16_t offset) {
    byte *head = reinterpret_cast<byte *>(this) + attr_offsets()[offset];
    return reinterpret_cast<MiniBlock *>(head);
  }

  uint32_t &num_slots() {
    return *reinterpret_cast<uint32_t *>(varlen_contents_);
  }

  /* Helper methods to navigate fields in the header */
  uint32_t *attr_offsets() {
    return &num_slots() + 1;
  }

  uint16_t &num_attrs(const BlockLayout &layout) {
    return *reinterpret_cast<uint16_t *>(attr_offsets()[layout.num_attrs_]);
  }

  uint8_t *attr_sizes(const BlockLayout &layout) {
    return reinterpret_cast<uint8_t *>(num_attrs(layout) + 1);
  }

  block_id_t block_id_;
  uint32_t num_records_;
  // Because where the other fields start will depend on the specific layout,
  // reinterpreting the rest as bytes is the best we can do without LLVM.
  byte varlen_contents_[0];
};
}

class TupleAccessStrategy {
 public:
  TupleAccessStrategy(BlockLayout layout) : layout_(std::move(layout)) {}

  // TODO(Tianyu): We are taking in a RawBlock * and an offset, instead of a
  // TupleSlot because presumably someone would have already looked up a block_id,
  // checked the schema and found the right TupleAccessStrategy. Or, someone
  // might call this accessor multiple times in sequence on the same tuple id.
  // It doesn't make sense to encapsulate the lookup in this class.

  /* Vectorized Access */
  RawConcurrentBitmap *ColumnNullBitmap(RawBlock *block,
                                        uint16_t column_offset) {
    return reinterpret_cast<Block *>(block)->Column(column_offset)->NullBitmap();
  }

  byte *ColumnStart(RawBlock *block, uint16_t column_offset) {
    return reinterpret_cast<Block *>(block)
        ->Column(column_offset)
        ->ColumnStart(layout_);
  }

  /* Tuple-level access */
  // TODO(Tianyu): Roughly what the API should look like?
  byte *AccessWithNullCheck(RawBlock *block,
                            uint16_t column_offset,
                            uint32_t offset) {
    if (!ColumnNullBitmap(block, column_offset)->Test(offset))
      return nullptr;
    return ColumnStart(block, column_offset)
        + layout_.attr_sizes_[column_offset] * offset;
  }

  byte *AccessForceNotNull(RawBlock *block,
                           uint16_t column_offset,
                           uint32_t offset) {
    // Noop if not null
    ColumnNullBitmap(block, column_offset)->Flip(offset, false);
    return ColumnStart(block, column_offset)
        + layout_.attr_sizes_[column_offset] * offset;
  }

  void SetNull(RawBlock *block, uint16_t column_offset, uint32_t offset) {
    // Noop if already null
    ColumnNullBitmap(block, column_offset)->Flip(offset, true);
  }

  void MarkFree(RawBlock *block, uint16_t column_offset) {
    // Marking the Primary Key as null is the same as freeing
    SetNull(block, column_offset, PRIMARY_KEY_OFFSET);
  }

  /* Allocation and Deallocation */
  bool Allocate(RawBlock *block, uint32_t &offset) {
    // TODO(Tianyu): Really inefficient for now. Again, embarrassingly vectorizable.
    // Optimize later.
    RawConcurrentBitmap *bitmap = ColumnNullBitmap(block, PRIMARY_KEY_OFFSET);
    for (uint32_t i = 0; i < layout_.num_slots_; i++) {
      if (bitmap->Flip(i, false)) {
        offset = i;
        return true;
      }
    }
    return false;
  }

 private:
  // TODO(Tianyu): This will be baked in for codegen, not a field.
  const BlockLayout layout_;
};
}
}
