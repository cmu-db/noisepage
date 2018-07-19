#pragma once
#include <sstream>
#include "common/printable.h"
#include "common/common_defs.h"
#include "common/macros.h"

namespace terrier {
STRONG_TYPEDEF(block_id_t, uint32_t)
/**
 * A TupleSlot represents a physical location of a tuple in memory.
 */
struct TupleSlot : public Printable {
  block_id_t block_id_;
  uint32_t offset_;

  const std::string GetInfo() const override {
    std::ostringstream out;
    out << "block id: " << block_id_ << ", offset: " << offset_ << std::endl;
    return out.str();
  }
};

// TODO(Tianyu): We probably want to align this to some level
/**
 * A block is a chunk of memory used for storage. It does not have any meaning
 * unless interpreted by a @see TupleAccessStrategy
 */
class RawBlock {
 public:
  RawBlock() {
    // Intentionally unused
    (void) content_;
  }
  byte content_[Constants::BLOCK_SIZE];
};
}