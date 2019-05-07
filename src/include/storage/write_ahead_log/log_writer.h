#pragma once

#include <thread>
#include "storage/write_ahead_log/log_io.h"

namespace terrier::storage {

// Forward declaration for class LogManager
class LogManager;
/**
 * A LogWriter is responsible for writing serialized log records out to disk.
 * @param log_manager pointer to the LogManager
 */
class LogWriter {
 public:
  /**
   * Constructs a new LogWriter
   */
  explicit LogWriter(LogManager *log_manager) : log_manager_(log_manager) {
    log_writer_thread_ = std::thread([this] { WriteToDisk(); });
  }

  /**
   * Shuts down the LogWriter thread. Must be called only from Shutdown of LogManager
   */
  void Shutdown() { log_writer_thread_.join(); }

 private:
  // The log writer thread which flushes filled buffers to the disk
  std::thread log_writer_thread_;

  LogManager *const log_manager_;

  /**
   * Flush all buffers in the filled buffers queue to the disk, followed by an fsync
   */
  void FlushAllBuffers();

  /**
   * Write data to disk till shutdown. This is what the log writer thread runs
   */
  void WriteToDisk();
};
}  // namespace terrier::storage
