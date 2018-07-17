#pragma once

#include <vector>
#include "common/concurrent_bitmap.h"
#include "common/macros.h"
#include "storage/storage_defs.h"
#include "storage/block_store.h"

namespace terrier {
namespace storage {
enum class AttributeSize : uint8_t {
  ONE = 1,
  TWO = 2,
  FOUR = 4,
  EIGHT = 8,
  VARIABLE = 255 // Realistically just 8 bytes of ptr to a varlen pool
};

uint8_t ByteSize(const AttributeSize &size) {
  return (size == AttributeSize::VARIABLE) ? sizeof(byte *)
                                           : static_cast<uint8_t>(size);
}

// TODO(Tianyu): This code eventually should be compiled, which would eliminate
// BlockLayout as a runtime object, instead baking them in as compiled code
// (Think of this as writing the class with a BlockLayout template arg, except
// template instantiation is done by LLVM at runtime and not at compile time.
struct BlockLayout {
  BlockLayout(uint16_t num_attrs, std::vector<const AttributeSize> attr_sizes)
      : num_attrs_(num_attrs),
        attr_sizes_(std::move(attr_sizes)),
        tuple_size_(ComputeTupleSize()) {}

  const uint16_t num_attrs_;
  const std::vector<const AttributeSize> attr_sizes_;
  // Cached so we don't have to iterate through attr_sizes every time
  const uint32_t tuple_size_ = 0;

 private:
  uint32_t ComputeTupleSize() {
    PELOTON_ASSERT(num_attrs_ == attr_sizes_.size());
    uint32_t result = 0;
    for (auto size : attr_sizes_)
      result += ByteSize(size);
    return result;
  }
};
// TODO(Tianyu): These should be aligned for LLVM

// For individual columns
// Mini block layout:
// ------------------------------------------------------------
// | null-bitmap (pad up to byte-aligned) | val1 | val2 | ... |
// ------------------------------------------------------------
class MiniBlock {
 public:
  byte *GetAttr(uint32_t, uint32_t) {
    return nullptr;
  }

 private:
  MiniBlock() {
    (void) varlen_contents_;
  }

  RawConcurrentBitmap &null_bitmap() {
    return *reinterpret_cast<RawConcurrentBitmap *>(varlen_contents_);
  }

  template<typename T>
  T *columns(uint32_t num_columns) {
    return reinterpret_cast<T *>(varlen_contents_ + BitmapSize(num_columns));
  }
  // Because where the other fields start will depend on the specific layout,
  // reinterpreting the rest as bytes is the best we can do without LLVM.
  byte varlen_contents_[0]{};
};

// Block Header layout:
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
class PACKED Block {
  // This is packed because these will only be constructed from compact
  // storage bytes, not on the fly.
 public:
  static Block *Initialize(RawBlock *raw,
                           const BlockLayout &layout,
                           block_id_t block_id);
  MiniBlock *Column(uint16_t offset) {
    byte *head = reinterpret_cast<byte *>(this) + attr_offsets()[offset];
    return reinterpret_cast<MiniBlock *>(head);
  }

 private:
  friend class TupleAccessStrategy;
  static uint32_t HeaderSize(const BlockLayout &layout) {
    return sizeof(Block)
        + layout.num_attrs_ * sizeof(uint32_t)
        + sizeof(uint16_t)
        + layout.num_attrs_ * sizeof(AttributeSize);
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

  AttributeSize *attr_sizes(const BlockLayout &layout) {
    return reinterpret_cast<AttributeSize *>(num_attrs(layout) + 1);
  }

  block_id_t block_id_;
  uint32_t num_records_;
  // Because where the other fields start will depend on the specific layout,
  // reinterpreting the rest as bytes is the best we can do without LLVM.
  byte varlen_contents_[0];
};

class TupleAccessStrategy {
 public:
  TupleAccessStrategy(BlockLayout layout,
                      BlockStore &store)
      : layout_(std::move(layout)), store_(store) {
    (void) layout_;
    (void) store_;
  }

  // TODO(Tianyu): Roughly what the API should look like?
  byte *Access(TupleSlot slot, uint16_t column_offset) {
    auto *block =
        reinterpret_cast<Block *>(store_.RetrieveBlock(slot.block_id_));
    return block->Column(column_offset)
                ->GetAttr(slot.offset_, block->num_records_);
  }

  void Allocate(uint32_t, TupleSlot *) {}

  void UnsafeDeallocate(TupleSlot) {}

 private:
  // TODO(Tianyu): This will be baked in for codegen, not a field.
  const BlockLayout layout_;
  BlockStore &store_;
};
}
}
