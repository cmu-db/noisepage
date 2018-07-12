#pragma once
#include <sstream>
#include "common/printable.h"
#include "common/common_defs.h"
#include "common/macros.h"

namespace terrier {
STRONG_TYPEDEF(block_id_t, uint32_t)
struct TupleId : public Printable {
  block_id_t block_id_;
  uint32_t offset_;

  const std::string GetInfo() const override {
    std::ostringstream out;
    out << "block id: " << block_id_ << ", offset: " << offset_ << std::endl;
    return out.str();
  }
};

class RawBlock {
  byte content_[Constants::BLOCK_SIZE];
};

// TODO(Tianyu): we don't want to actually write code this way, but it will
// help us to keep in mind what's compiled in and what's not as we write out the
// ideas.
//template <uint16_t num_attributes>
//struct BlockLayout {
//  uint8_t attr_sizes_[num_attributes];
//};
//
//template <uint32_t num_slots>
//class MiniBlock {
// public:
//
// private:
//  std::bitset<num_slots> null_map_;
//  byte contents_[0];
//};
//
//template <uint32_t num_attrs, uint32_t num_slots, BlockLayout<num_attrs> layout>
//class Block {
// public:
//  static Block *Initialize(block_id_t block_id, RawBlock *raw) {
//    auto *result = reinterpret_cast<Block *>(raw);
//    result->block_id_ = block_id;
//    result->num_attributes_ = num_attrs;
//    for (uint32_t i; i < num_attrs; i++)
//      result->attr_sizes[i] = layout.attr_sizes_[i];
//    // TODO(Tianyu): Layout columns
//  }
//
//
//
//  MiniBlock<num_slots>  *GetColumn(uint32_t column_offset) {
//    return reinterpret_cast<MiniBlock<num_slots>  *>(contents_ + column_heads_[column_offset]);
//  }
//
// private:
//  Block() = delete;
//  ~Block() = delete;
//  DISALLOW_COPY_AND_MOVE(Block);
//
//  // TODO(Tianyu): We are assuming that c++ will not pad out any of these members
//  // when laying the object out, which requires this ordering. Whether this is
//  // a good thing to do is debatable.
//  block_id_t block_id_;
//  uint32_t num_attributes_;
//  uint8_t attr_sizes[num_attrs];
//  uint16_t column_heads_[num_attrs];
//  // TODO(Tianyu): Need to talk about recycling / reuse slots
//  uint32_t num_tuples_, num_slots_;
//  byte contents_[0];
//};
}