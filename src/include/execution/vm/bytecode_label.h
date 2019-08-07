#pragma once

#include <limits>
#include <vector>

#include "execution/util/macros.h"

namespace terrier::execution::vm {

/**
 * A label represents a location in the bytecode and is used as the target of a
 * jump instruction. When the label is bound, it becomes an immutable reference
 * to a location in the bytecode (accessible through @ref offset()). If the
 * label is a forward target, @ref referrer_offsets() will return the bytecode location
 * of the referring jump instruction.
 */
class BytecodeLabel {
  /**
   * Invalid offset. Used for unbound labels.
   */
  static constexpr const std::size_t kInvalidOffset = std::numeric_limits<std::size_t>::max();

 public:
  /**
   * Constructor. Does not yet bind the label
   */
  BytecodeLabel() : offset_(kInvalidOffset) {}

  /**
   * @return whether the label is bound ot not.
   */
  bool is_bound() const { return bound_; }

  /**
   * @return the bytecode offset of the label.
   */
  std::size_t offset() const { return offset_; }

  /**
   * @return the list of referrer offsets.
   */
  const std::vector<size_t> &referrer_offsets() const { return referrer_offsets_; }

  /**
   * @return whether the label is a forward target.
   */
  bool IsForwardTarget() const { return !is_bound() && !referrer_offsets().empty(); }

 private:
  friend class BytecodeEmitter;

  void set_referrer(std::size_t offset) {
    TPL_ASSERT(!is_bound(), "Cannot set offset reference for already bound label");
    referrer_offsets_.push_back(offset);
  }

  void BindTo(std::size_t offset) {
    TPL_ASSERT(!is_bound() && offset != kInvalidOffset, "Cannot rebind an already bound label!");
    bound_ = true;
    offset_ = offset;
  }

 private:
  std::size_t offset_;
  std::vector<size_t> referrer_offsets_;
  bool bound_{false};
};

}  // namespace terrier::execution::vm
