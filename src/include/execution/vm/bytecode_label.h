#pragma once

#include <limits>
#include <vector>

#include "common/macros.h"

namespace terrier::execution::vm {

/**
 * A label represents a location in the bytecode and is used as the target of a
 * jump instruction. When the label is bound, it becomes an immutable reference
 * to a location in the bytecode (accessible through @ref Offset()). If the
 * label is a forward target, @ref ReferrerOffsets() will return the bytecode location
 * of the referring jump instruction.
 */
class BytecodeLabel {
  /**
   * Invalid offset. Used for unbound labels.
   */
  static constexpr const std::size_t K_INVALID_OFFSET = std::numeric_limits<std::size_t>::max();

 public:
  /**
   * Constructor. Does not yet bind the label
   */
  BytecodeLabel() : offset_(K_INVALID_OFFSET) {}

  /**
   * @return whether the label is bound ot not.
   */
  bool IsBound() const { return bound_; }

  /**
   * @return the bytecode offset of the label.
   */
  std::size_t Offset() const { return offset_; }

  /**
   * @return the list of referrer offsets.
   */
  const std::vector<size_t> &ReferrerOffsets() const { return referrer_offsets_; }

  /**
   * @return whether the label is a forward target.
   */
  bool IsForwardTarget() const { return !IsBound() && !ReferrerOffsets().empty(); }

 private:
  friend class BytecodeEmitter;

  void SetReferrer(std::size_t offset) {
    TERRIER_ASSERT(!IsBound(), "Cannot set offset reference for already bound label");
    referrer_offsets_.push_back(offset);
  }

  void BindTo(std::size_t offset) {
    TERRIER_ASSERT(!IsBound() && offset != K_INVALID_OFFSET, "Cannot rebind an already bound label!");
    bound_ = true;
    offset_ = offset;
  }

 private:
  std::size_t offset_;
  std::vector<size_t> referrer_offsets_;
  bool bound_{false};
};

}  // namespace terrier::execution::vm
