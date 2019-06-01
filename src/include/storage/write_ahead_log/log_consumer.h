#pragma once

#include <common/dedicated_thread_registry.h>
#include "storage/write_ahead_log/log_io.h"

namespace terrier::storage {

// Forward declaration for class LogManager
class LogManager;

/**
 * A LogConsumerTask is responsible for writing serialized log records out to disk by processing buffers in the log
 * manager's filled buffer queue
 */
class LogConsumerTask : public DedicatedThreadTask {
 public:
  /**
   * Constructs a new LogConsumerTask
   * @param log_manager pointer to the LogManager
   */
  explicit LogConsumerTask(LogManager *log_manager) : log_manager_(log_manager) {}

  virtual ~LogConsumerTask() = default;

  void RunTask() override;

  void Terminate() override;

 private:
  // Log manager that created this task
  LogManager *const log_manager_;
  bool run_consumer_task_;

  /**
   * Main log consumer loop. Flushes buffers to disk when new buffers are handed to it via filled_buffer_queue_, or when
   * notified by LogManager to persist buffers
   */
  void LogConsumerTaskLoop();

  /**
   * Flush all buffers in the filled buffers queue to the log file
   */
  void FlushAllBuffers();

  /*
   * Persists the log file on disk by calling fsync
   */
  void PersistAllBuffers();
};
}  // namespace terrier::storage
