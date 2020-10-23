#pragma once

#include <limits>
#include <vector>

#include "common/macros.h"

namespace noisepage::execution::vm {

/**
 * A label represents a location in the bytecode and is used as the target of a jump instruction.
 * When the label is bound, it becomes an immutable reference to a location in the bytecode
 * accessible through BytecodeLabel::GetOffset(). If the label is a forward target,
 * BytecodeLabel::GetOffset() will return the bytecode location of the referring jump instruction.
 */
class BytecodeLabel {
  static constexpr const std::size_t INVALID_OFFSET = std::numeric_limits<std::size_t>::max();

 public:
  /**
   * Construct an unbound label with no referrers.
   */
  BytecodeLabel() = default;

  /**
   * @return True if the label is bound (to a fixed bytecode position); false otherwise.
   */
  bool IsBound() const noexcept { return bound_; }

  /**
   * @return The offset this label is bound to, if bound at all.
   */
  std::size_t GetOffset() const noexcept { return offset_; }

  /**
   * @return The list of other positions in the bytecode that refer (i.e., jump) to this label.
   */
  const std::vector<std::size_t> &GetReferrerOffsets() const noexcept { return referrer_offsets_; }

  /**
   * @return True if this label is a forward target. A forward target is one that has yet to be
   *         bound (because we haven't reached that bytecode position), and if there are other
   *         bytecode positions that refer (i.e., jump) to this label.
   */
  bool IsForwardTarget() const noexcept { return !IsBound() && !GetReferrerOffsets().empty(); }

 private:
  friend class BytecodeEmitter;

  void SetReferrer(const std::size_t offset) {
    NOISEPAGE_ASSERT(!IsBound(), "Cannot set offset reference for already bound label");
    referrer_offsets_.push_back(offset);
  }

  void BindTo(std::size_t offset) {
    NOISEPAGE_ASSERT(!IsBound() && offset != INVALID_OFFSET, "Cannot rebind an already bound label!");
    bound_ = true;
    offset_ = offset;
  }

 private:
  // The bytecode offset of this label
  std::size_t offset_{INVALID_OFFSET};

  // The list of positions that jump to this label
  std::vector<std::size_t> referrer_offsets_;

  // Flag indicating if the label has been bound to a position/offset
  bool bound_{false};
};

}  // namespace noisepage::execution::vm
