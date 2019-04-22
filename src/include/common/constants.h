#pragma once

#include <cstdint>

namespace terrier::common {
/**
 * Declare all system-level constants that cannot change at runtime here.
 */
struct Constants {
  /**
   * Block/RawBlock size, in bytes. Must be a power of 2.
   */
  static const uint32_t BLOCK_SIZE = 1 << 20;
  /**
   * Buffer segment size, in bytes.
   */
  static const uint32_t BUFFER_SEGMENT_SIZE = 1 << 12;
  /**
   * Maximum number of columns a table is allowed to have. It should be sufficiently small such that  if all
   * columns are as large as they can be there is still at last one slot for every block.
   */
  // TODO(Tianyu): This number currently is obtained through empirical experiments.
  static const uint16_t MAX_COL = 12500;
};
}  // namespace terrier::common
