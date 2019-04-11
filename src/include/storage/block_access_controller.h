#pragma once
#include <atomic>
#include <utility>
#include "common/macros.h"
#include "common/strong_typedef.h"

namespace terrier::storage {
/**
 * TODO(Tianyu): Placeholder for the actual implementation that has the same number of bytes. We use this to make sure
 *               the new offset calculations and everything works.
 */
class BlockAccessController {
 public:
  /**
   * TODO(Tianyu): Make GCC happy
   */
  uint64_t bytes_;
};
}  // namespace terrier::storage
