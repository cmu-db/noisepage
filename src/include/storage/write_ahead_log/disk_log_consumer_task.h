#pragma once

#include <utility>
#include <vector>
#include "common/container/concurrent_blocking_queue.h"
#include "common/dedicated_thread_registry.h"
#include "storage/storage_defs.h"
#include "storage/write_ahead_log/log_io.h"

namespace terrier::storage {

/**
 * A DiskLogConsumerTask is responsible for writing serialized log records out to disk by processing buffers in the log
 * manager's filled buffer queue
 */
class DiskLogConsumerTask : public DedicatedThreadTask {
 public:
  /**
   * Constructs a new DiskLogConsumerTask
   * @param log_manager pointer to the LogManager
   */
  explicit DiskLogConsumerTask(std::vector<BufferedLogWriter> *buffers,
                               common::ConcurrentBlockingQueue<BufferedLogWriter *> *empty_buffer_queue,
                               common::ConcurrentQueue<storage::SerializedLogs> *filled_buffer_queue)
      : run_task_(false),
        buffers_(buffers),
        empty_buffer_queue_(empty_buffer_queue),
        filled_buffer_queue_(filled_buffer_queue) {}

  /**
   * Runs main disk log writer loop. Called by thread registry upon initialization of thread
   */
  void RunTask() override;

  /**
   * Signals task to stop. Called by thread registry upon termination of thread
   */
  void Terminate() override;

 private:
  friend class LogManager;
  // Flag to signal task to run or stop
  bool run_task_;
  // Stores callbacks for commit records written to disk but not yet persisted
  std::vector<storage::CommitCallback> commit_callbacks_;

  // This stores a reference to all the buffers the log manager has created. Used for persisting
  std::vector<BufferedLogWriter> *buffers_;
  // The queue containing empty buffers. Task will enqueue a buffer into this queue when it has flushed its logs
  common::ConcurrentBlockingQueue<BufferedLogWriter *> *empty_buffer_queue_;
  // The queue containing filled buffers. Task should dequeue filled buffers from this queue to flush
  common::ConcurrentQueue<SerializedLogs> *filled_buffer_queue_;

  // Flag used by the serializer thread to signal the disk log consumer task thread to persist the data on disk
  volatile bool do_persist_;

  // Synchronisation primitives to synchronise persisting buffers to disk
  std::mutex persist_lock_;
  std::condition_variable persist_cv_;
  // Condition variable to signal disk log consumer task thread to wake up and flush buffers to disk or if shutdown has
  // initiated, then quit
  std::condition_variable disk_log_writer_thread_cv_;

  /**
   * Main disk log consumer task loop. Flushes buffers to disk when new buffers are handed to it via
   * filled_buffer_queue_, or when notified by LogManager to persist buffers
   */
  void DiskLogConsumerTaskLoop();

  /**
   * Flush all buffers in the filled buffers queue to the log file
   */
  void FlushAllBuffers();

  /*
   * Persists the log file on disk by calling fsync, as well as calling callbacks for all committed transactions that
   * were persisted
   */
  void PersistAllBuffers();
};
}  // namespace terrier::storage
