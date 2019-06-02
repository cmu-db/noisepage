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
#include "storage/record_buffer.h"
#include "storage/write_ahead_log/log_consumer.h"
#include "storage/write_ahead_log/log_io.h"
#include "storage/write_ahead_log/log_record.h"
#include "transaction/transaction_defs.h"

// TODO(Utkarsh): Get rid of magic constants
#define MAX_BUF 2

namespace terrier::storage {
/**
 * A LogManager is responsible for serializing log records out and keeping track of whether changes from a transaction
 * are persistent.
 */
class LogManager : public DedicatedThreadOwner {
 public:
  /**
   * Constructs a new LogManager, writing its logs out to the given file.
   *
   * @param log_file_path path to the desired log file location. If the log file does not exist, one will be created;
   *                      otherwise, changes are appended to the end of the file.
   * @param buffer_pool the object pool to draw log buffers from. This must be the same pool transactions draw their
   *                    buffers from
   */
  LogManager(const char *log_file_path, RecordBufferSegmentPool *const buffer_pool)
      : run_log_manager_(false),
        buffer_pool_(buffer_pool),
        log_file_path_(log_file_path),
        filled_buffer_(nullptr),
        do_persist_(true) {}

  ~LogManager() { delete log_consumer_task_; }

  /**
   * Start logging
   */
  void Start() {
    // Initialize buffers for logging
    for (int i = 0; i < MAX_BUF; i++) {
      buffers_.emplace_back(BufferedLogWriter(log_file_path_));
    }
    for (int i = 0; i < MAX_BUF; i++) {
      empty_buffer_queue_.Enqueue(&buffers_[i]);
    }

    run_log_manager_ = true;

    // Register consumer task
    log_consumer_task_ = new LogConsumerTask(this);
    DedicatedThreadRegistry::GetInstance().RegisterDedicatedThread(
        this, common::ManagedPointer<DedicatedThreadTask>(log_consumer_task_));
  }

  /**
   * Must be called when no other threads are doing work
   */
  void Shutdown();

  /**
   * Returns a (perhaps partially) filled log buffer to the log manager to be consumed. Caller should drop its
   * reference to the buffer after the method returns immediately, as it would no longer be safe to read from or
   * write to the buffer. This method can be called safely from concurrent execution threads.
   *
   * @param buffer_segment the (perhaps partially) filled log buffer ready to be consumed
   */
  void AddBufferToFlushQueue(RecordBufferSegment *const buffer_segment) {
    common::SpinLatch::ScopedSpinLatch guard(&flush_queue_latch_);
    flush_queue_.push(buffer_segment);
  }

  /**
   * Process all the accumulated log records and serialize them out to disk. A flush will always happen at the end.
   * (Beware the performance consequences of calling flush too frequently) This method should only be called from a
   * dedicated
   * logging thread.
   */
  void Process();

  /**
   * Flush the logs to make sure all serialized records before this invocation are persistent. Callbacks from committed
   * transactions are also invoked when possible. This method should only be called from a dedicated logging thread.
   *
   * Usually this method is called from Process(), but can also be called by itself if need be.
   */
  void Flush();

 private:
  friend class LogConsumerTask;

  bool run_log_manager_;

  // TODO(Tianyu): This can be changed later to be include things that are not necessarily backed by a disk
  //  (e.g. logs can be streamed out to the network for remote replication)
  RecordBufferSegmentPool *buffer_pool_;

  // TODO(Tianyu): Might not be necessary, since commit on txn manager is already protected with a latch
  common::SpinLatch flush_queue_latch_;
  // TODO(Tianyu): benchmark for if these should be concurrent data structures, and if we should apply the same
  //  optimization we applied to the GC queue.
  std::queue<RecordBufferSegment *> flush_queue_;

  // These do not need to be thread safe since the only thread adding or removing from it is the flushing thread
  std::vector<std::pair<transaction::callback_fn, void *>> commits_in_buffer_;

  // System path for log file
  const char *log_file_path_;
  // This stores all the buffers the serializer or the log consumer threads use
  std::vector<BufferedLogWriter> buffers_;
  // This is the buffer the serializer thread will write to
  BufferedLogWriter *filled_buffer_;
  // The queue containing empty buffers which the serializer thread will use
  common::ConcurrentBlockingQueue<BufferedLogWriter *> empty_buffer_queue_;
  // The queue containing filled buffers pending flush to the disk
  common::ConcurrentBlockingQueue<BufferedLogWriter *> filled_buffer_queue_;

  // The log consumer task which flushes filled buffers to the disk
  LogConsumerTask *log_consumer_task_ = nullptr;
  // Flag used by the serializer thread to signal the log consumer thread to persist the data on disk
  volatile bool do_persist_;

  // Synchronisation primitives to synchronise persisting buffers to disk
  std::mutex persist_lock_;
  std::condition_variable persist_cv_;
  // Condition variable to signal consumer thread to wake up and flush buffers to disk or if shutdown has initiated,
  // then quit
  std::condition_variable consumer_thread_cv_;

  /**
   * Serialize out the record to the log
   * @param record the redo record to serialise
   */
  void SerializeRecord(const LogRecord &record);

  /**
   * Serialize out the task buffer to the log
   * @param buffer_to_serialize the iterator to the redo buffer to be serialized
   */
  void SerializeBuffer(IterableBufferSegment<LogRecord> *buffer_to_serialize);

  /**
   * Used by the serializer thread to get a buffer to serialize data to
   * @return buffer to write to
   */
  BufferedLogWriter *GetCurrentWriteBuffer() {
    if (filled_buffer_ == nullptr) {
      empty_buffer_queue_.Dequeue(&filled_buffer_);
    }
    return filled_buffer_;
  }

  /**
   * Serialize the data pointed to by val to a buffer
   * @tparam T Type of the value
   * @param val The value to write to the buffer
   */
  template <class T>
  void WriteValue(const T &val) {
    WriteValue(&val, sizeof(T));
  }

  /**
   * Serialize the data pointed to by val to a buffer
   * @param val the value
   * @param size size of the value to serialize
   */
  void WriteValue(const void *val, uint32_t size);

  /**
   * Mark the current buffer that the serializer thread is writing to as filled
   */
  void HandFilledBufferToWriter() {
    filled_buffer_queue_.Enqueue(filled_buffer_);
    // Signal consumer thread that a buffer is ready to be flushed to the disk
    {
      std::unique_lock<std::mutex> lock(persist_lock_);
      consumer_thread_cv_.notify_one();
    }
    // Mark that serializer thread doesn't have a buffer in its possession to which it can write to
    filled_buffer_ = nullptr;
  }

  /**
   * If the central dispatch removes our log consumer thread, we need to request a new one
   * @param task the task that was removed by the registry from the thread we were originally using
   */
  void OnThreadRemoved(common::ManagedPointer<DedicatedThreadTask> task) override {
    TERRIER_ASSERT(task == log_consumer_task_, "Log manager should only be given back it's log consumer task");
    // We don't want to register a task if the log manager is shutting down though.
    if (run_log_manager_) {
      // Because the task itself does not keep metadata, we can simply reuse the task.
      DedicatedThreadRegistry::GetInstance().RegisterDedicatedThread(
          this, common::ManagedPointer<DedicatedThreadTask>(log_consumer_task_));
    }
  }
};
}  // namespace terrier::storage
