#pragma once
#include "common/macros.h"
#include "common/serializable.h"
#include "logging/log_record.h"

namespace terrier::logging {

/**
 * A LogBuffer is a container for LogRecords. It is free when required by work
 * threads from a LogManager, and then filled by them with LogRecords. It will
 * be returned to the LogManger when either it gets full or a transaction
 * commits.
 */
class LogBuffer {
 public:
  /**
   * @brief A LogBuffer is only initiated with parameters when created,
   * and should never be copied or moved.
   */
  DISALLOW_COPY_AND_MOVE(LogBuffer);

  /**
   * @brief Constructs a buffer with the given threshold.
   *
   * @param threshold the size beyond which the buffer will be flushed
   */
  explicit LogBuffer(uint32_t threshold) : threshold_(threshold) {}

  /**
   * @brief Returns whether the size of the buffer exceeds the threshold.
   *
   * @return true if the size of the buffer exceeds the threshold, false
   * otherwise.
   */
  bool IsExceeded() { return buffer_.Size() >= threshold_; }

  /**
   * @brief Writes a log record to the buffer.
   *
   * @param record the log record to be written
   */
  void WriteRecord(const LogRecord &record);

 private:
  CopySerializeOutput buffer_;
  uint32_t threshold_;
};
}  // namespace terrier::logging
