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

  /**
   * The size of the buffers the log manager uses to buffer serialized logs and "group commit" them when writing to disk
   */
  static const uint32_t LOG_BUFFER_SIZE = (1 << 12);

  /**
   * The cache line size in bytes
   */
  static const uint8_t CACHELINE_SIZE = 64;

  /**
   * The number of bits per byte
   */
  static constexpr const uint32_t K_BITS_PER_BYTE = 8;

  /**
   * The default vector size to use when performing vectorized iteration
   */
  static constexpr const uint32_t K_DEFAULT_VECTOR_SIZE = 2048;

  /**
   * The default prefetch distance to use
   */
  static constexpr const uint32_t K_PREFETCH_DISTANCE = 16;

  // Common memory sizes
  /**
   * KB
   */
  static constexpr const uint32_t KB = 1024;

  /**
   * MB
   */
  static constexpr const uint32_t MB = KB * KB;

  /**
   * GB
   */
  static constexpr const uint32_t GB = KB * KB * KB;
};
}  // namespace terrier::common
