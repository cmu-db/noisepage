#pragma once

#include <memory>
#include <queue>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include "common/container/concurrent_blocking_queue.h"
#include "common/dedicated_thread_owner.h"
#include "common/managed_pointer.h"
#include "common/spin_latch.h"
#include "common/strong_typedef.h"
#include "settings/settings_manager.h"
#include "storage/record_buffer.h"
#include "storage/write_ahead_log/disk_log_consumer_task.h"
#include "storage/write_ahead_log/log_flusher_task.h"
#include "storage/write_ahead_log/log_io.h"
#include "storage/write_ahead_log/log_record.h"
#include "transaction/transaction_defs.h"

namespace terrier::storage {

// Forward declaration for class DiskLogConsumerTask
class DiskLogConsumerTask;
class LogSerializerTask;
class LogFlusherTask;

/**
 * Callback functionn and arguments to be called when record is persisted
 */
using CommitCallback = std::pair<transaction::callback_fn, void *>;

/**
 * A BufferedLogWriter containing serialized logs, as well as all commit callbacks for transaction's whose commit are
 * serialized in this BufferedLogWriter
 */
using SerializedLogs = std::pair<BufferedLogWriter *, std::vector<CommitCallback>>;

/**
 * A LogManager is responsible for serializing log records out and keeping track of whether changes from a transaction
 * are persistent. The standard flow of a log record from a transaction all the way to disk is as follows:
 *      1. The LogManager receives buffers containing records from transactions via the AddBufferToFlushQueue, and
 * adds them to a flush queue (flush_queue_)
 *      2. The LogSerializerTask will periodically call Process() to process and serialize buffers in the flush queue
 * and hand them over to the consumer queue (filled_buffer_queue_). The reason this is done offline and not as soon as
 * logs are received is to reduce the amount of time a transaction spends interacting with the log manager
 *      3. When a buffer of logs is handed over to a consumer, the consumer will wake up and process the logs. In the
 * case of the DiskLogConsumerTask, this means writing it to the log file.
 *      4. The LogFlusher task will periodically call ForceFlush() to persist the log file to disk using fsync. Once
 * this is done, the commit callback on any persisted logs will be called.
 */
class LogManager : public DedicatedThreadOwner {
 public:
  /**
   * Constructs a new LogManager, writing its logs out to the given file.
   *
   * @param log_file_path path to the desired log file location. If the log file does not exist, one will be created;
   *                      otherwise, changes are appended to the end of the file.
   * @param num_buffers Number of buffers to use for buffering logs
   * @param serialization_interval Interval time between log serializations
   * @param flushing_interval Interval time between log flushing
   * @param buffer_pool the object pool to draw log buffers from. This must be the same pool transactions draw their
   *                    buffers from
   */
  LogManager(std::string log_file_path, uint64_t num_buffers, const std::chrono::milliseconds serialization_interval,
             const std::chrono::milliseconds flushing_interval, RecordBufferSegmentPool *const buffer_pool)
      : run_log_manager_(false),
        log_file_path_(std::move(log_file_path)),
        num_buffers_(num_buffers),
        buffer_pool_(buffer_pool),
        serialization_interval_(serialization_interval),
        flushing_interval_(flushing_interval),
        do_persist_(true) {}

  /**
   * Starts log manager. Does the following in order:
   *    1. Initialize buffers to pass serialized logs to log consumers
   *    2. Starts up DiskLogConsumerTask
   *    3. Starts up LogFlusherTask
   *    4. Starts up LogSerializerTask
   */
  void Start();

  /**
   * Flush the logs to make sure all serialized records before this invocation are persistent. Callbacks from committed
   * transactions are invoked by log consumers when the commit records are persisted on disk.
   * @warning This method should only be called from a dedicated flushing thread or during testing
   * @warning Beware the performance consequences of calling flush too frequently
   */
  void ForceFlush();

  /**
   * Persists all unpersisted logs and stops the log manager. Does what Start() does in reverse order:
   *    1. Stops LogSerializerTask
   *    2. Stops LogFlusherTask
   *    3. Stops DiskLogConsumerTask
   *    2. Closes all open buffers
   * Start() can be called after to run the log manager again, a new log manager does not need to be instatiated.
   */
  void PersistAndStop();

  /**
   * Returns a (perhaps partially) filled log buffer to the log manager to be consumed. Caller should drop its
   * reference to the buffer after the method returns immediately, as it would no longer be safe to read from or
   * write to the buffer. This method can be called safely from concurrent execution threads.
   *
   * @param buffer_segment the (perhaps partially) filled log buffer ready to be consumed
   */
  void AddBufferToFlushQueue(RecordBufferSegment *const buffer_segment);

  /**
   * For testing only
   * @return number of buffers used for logging
   */
  uint64_t TestGetNumBuffers() { return num_buffers_; }

  /**
   * Set the number of buffers used for buffering logs.
   *
   * The operation fails if the LogManager has already allocated more buffers than the new size
   *
   * @param new_num_buffers the new number of buffers the log manager can use
   * @return true if new_num_buffers is successfully set and false the operation fails
   */
  bool SetNumBuffers(uint64_t new_num_buffers) {
    if (new_num_buffers >= num_buffers_) {
      // Add in new buffers
      for (size_t i = 0; i < new_num_buffers - num_buffers_; i++) {
        buffers_.emplace_back(BufferedLogWriter(log_file_path_.c_str()));
        empty_buffer_queue_.Enqueue(&buffers_[num_buffers_ + i]);
      }
      num_buffers_ = new_num_buffers;
      return true;
    }
    return false;
  }

 private:
  friend class DiskLogConsumerTask;
  friend class LogSerializerTask;
  friend class LogFlusherTask;

  // Flag to tell us when the log manager is running or during termination
  bool run_log_manager_;

  // System path for log file
  std::string log_file_path_;

  // Number of buffers to use for buffering and serializing logs
  uint64_t num_buffers_;

  // TODO(Tianyu): This can be changed later to be include things that are not necessarily backed by a disk
  //  (e.g. logs can be streamed out to the network for remote replication)
  RecordBufferSegmentPool *buffer_pool_;

  // These do not need to be thread safe since the only thread adding or removing from it is the flushing thread
  std::vector<std::pair<transaction::callback_fn, void *>> commits_in_buffer_;

  // This stores a reference to all the buffers the serializer or the log consumer threads use
  std::vector<BufferedLogWriter> buffers_;
  // The queue containing empty buffers which the serializer thread will use
  common::ConcurrentBlockingQueue<BufferedLogWriter *> empty_buffer_queue_;
  // The queue containing filled buffers pending flush to the disk
  common::ConcurrentBlockingQueue<SerializedLogs> filled_buffer_queue_;

  // Log serializer task that processes buffers handed over by transactions and serializes them into consumer buffers
  common::ManagedPointer<LogSerializerTask> log_serializer_task_ = common::ManagedPointer<LogSerializerTask>(nullptr);
  // Interval used by log serialization task
  const std::chrono::milliseconds serialization_interval_;

  // Log flusher task that periodically forces the DiskLogConsumerTask to persist the log file on disk
  common::ManagedPointer<LogFlusherTask> log_flusher_task_ = common::ManagedPointer<LogFlusherTask>(nullptr);
  // Interval used by log flushing task
  const std::chrono::milliseconds flushing_interval_;

  // The log consumer task which flushes filled buffers to the disk
  common::ManagedPointer<DiskLogConsumerTask> disk_log_writer_task_ =
      common::ManagedPointer<DiskLogConsumerTask>(nullptr);

  // Flag used by the serializer thread to signal the disk log consumer task thread to persist the data on disk
  volatile bool do_persist_;

  // Synchronisation primitives to synchronise persisting buffers to disk
  std::mutex persist_lock_;
  std::condition_variable persist_cv_;
  // Condition variable to signal disk log consumer task thread to wake up and flush buffers to disk or if shutdown has
  // initiated, then quit
  std::condition_variable disk_log_writer_thread_cv_;

  /**
   * If the central registry wants to removes our thread used for the disk log consumer task, we only allow removal if
   * we are in shut down, else we need to keep the task, so we reject the removal
   * @return true if we allowed thread to be removed, else false
   */
  bool OnThreadRemoval(common::ManagedPointer<DedicatedThreadTask> task) override {
    // We don't want to register a task if the log manager is shutting down though.
    return !run_log_manager_;
  }
};

}  // namespace terrier::storage
