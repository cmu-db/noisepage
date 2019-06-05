#pragma once

#include <common/dedicated_thread_registry.h>
#include "storage/write_ahead_log/log_io.h"
#include "storage/write_ahead_log/log_manager.h"

namespace terrier::storage {

// Forward declaration for class LogManager
class LogManager;

/**
 * A DiskLogWriterTask is responsible for writing serialized log records out to disk by processing buffers in the log
 * manager's filled buffer queue
 */
class DiskLogWriterTask : public DedicatedThreadTask {
 public:
  /**
   * Constructs a new DiskLogWriterTask
   * @param log_manager pointer to the LogManager
   */
  explicit DiskLogWriterTask(LogManager *log_manager) : log_manager_(log_manager) {}

  /**
   * Runs main disk log writer loop. Called by thread registry upon initialization of thread
   */
  void RunTask() override;

  /**
   * Signals task to stop. Called by thread registry upon termination of thread
   */
  void Terminate() override;

 private:
  // Log manager that created this task
  LogManager *const log_manager_;
  bool run_task_;
  std::vector<CommitCallback> commit_callbacks_;

  /**
   * Main disk log writer task loop. Flushes buffers to disk when new buffers are handed to it via filled_buffer_queue_,
   * or when notified by LogManager to persist buffers
   */
  void DiskLogWriterTaskLoop();

  /**
   * Flush all buffers in the filled buffers queue to the log file
   */
  void FlushAllBuffers();

  /*
   * Persists the log file on disk by calling fsync, as well as calling callbacks for all committed transactions that were persisted
   */
  void PersistAllBuffers();
};
}  // namespace terrier::storage
