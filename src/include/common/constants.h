#pragma once

#include <cstdint>

namespace terrier::common {
/**
 * Declare all system-level constants that cannot change at runtime here.
 */
struct Constants {
  /**
   * Block size, in bytes.
   */
  static const uint32_t BLOCK_SIZE = 1048576u;  // Should only ever be a power of 2.
  /**
   * Buffer segment size, in bytes.
   */
  // TODO(Tianyu): Size of this buffer can probably be tuned in later optimization runs.
  static const uint32_t BUFFER_SEGMENT_SIZE = 1 << 15;
  /**
   * Maximum number of columns a table is allowed to have.
   */
  static const uint16_t MAX_COL = INT16_MAX;
};
}  // namespace terrier::common
