#pragma once

#include <cstdint>
#include "hash_util.h"

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
   * The width of the buffer checksum
   */
  static const uint32_t LOG_BUFFER_SUM_WIDTH = sizeof(common::hash_t);
  /**
   * The width of the size of the payload that checksum covers
   */
  static const uint32_t LOG_BUFFER_PAYLOAD_SIZE_WIDTH = sizeof(uint16_t);
  /**
   * The width of the buffer information
   */
  static const uint32_t LOG_BUFFER_INFO_WIDTH = sizeof(uint32_t);
  /**
   * The size of the buffer payload
   */
  static const uint32_t LOG_BUFFER_PAYLOAD_SIZE = LOG_BUFFER_SIZE
      - LOG_BUFFER_SUM_WIDTH - LOG_BUFFER_PAYLOAD_SIZE_WIDTH - LOG_BUFFER_INFO_WIDTH;
};
}  // namespace terrier::common
