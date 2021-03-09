#pragma once

#include <string>

#include "storage/recovery/abstract_log_provider.h"
#include "storage/write_ahead_log/log_io.h"

namespace noisepage::storage {

/**
 * @brief Log provider for logs stored on disk
 * Provides logs to the recovery manager from logs persisted on disk. The log file is read in using the
 * BufferedLogReader.
 */
class DiskLogProvider : public AbstractLogProvider {
 public:
  /**
   * @param log_file_path path to log file to read logs from
   */
  explicit DiskLogProvider(const std::string &log_file_path) : in_(BufferedLogReader(log_file_path.c_str())) {}

  LogProviderType GetType() const override { return LogProviderType::DISK; }

 private:
  // Buffered log file reader
  storage::BufferedLogReader in_;

  /**
   * @return true if log file contains more records, false otherwise
   */
  bool HasMoreRecords() override { return in_.HasMore(); }

  /**
   * Read data from the log file into the destination provided
   * @param dest pointer to location to read into
   * @param size number of bytes to read
   * @return true if we read the given number of bytes
   */
  bool Read(void *dest, uint32_t size) override { return in_.Read(dest, size); }
};

}  // namespace noisepage::storage
