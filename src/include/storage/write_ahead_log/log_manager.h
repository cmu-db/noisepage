#pragma once

#include <queue>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include "common/container/concurrent_blocking_queue.h"
#include "common/spin_latch.h"
#include "common/strong_typedef.h"
#include "storage/record_buffer.h"
#include "storage/write_ahead_log/log_io.h"
#include "storage/write_ahead_log/log_record.h"
#include "storage/write_ahead_log/log_writer.h"
#include "transaction/transaction_defs.h"

// TODO(Utkarsh): Get rid of magic constants
#define MAX_BUF 2
#define QUEUE_WAIT_TIME_MILLISECONDS 10

namespace terrier::storage {
/**
 * A LogManager is responsible for serializing log records out and keeping track of whether changes from a transaction
 * are persistent.
 */
class LogManager {
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
      : buffer_pool_(buffer_pool),
        log_file_path_(log_file_path),
        buffer_to_write_(nullptr),
        log_writer_(nullptr),
        run_log_writer_thread_(false),
        do_persist_(true) {}

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
    run_log_writer_thread_ = true;
    log_writer_ = new LogWriter(this);
  }

  /**
   * Must be called when no other threads are doing work
   */
  void Shutdown() {
    Process();
    // Shutdown the log writer thread
    run_log_writer_thread_ = false;
    persist_and_empty_queue_cv_.notify_one();
    log_writer_->Shutdown();
    for (auto buf : buffers_) {
      buf.Close();
    }
    // Clear buffer queues
    BufferedLogWriter *tmp;
    while (!empty_buffer_queue_.Empty()) empty_buffer_queue_.Dequeue(&tmp);
    while (!filled_buffer_queue_.Empty()) filled_buffer_queue_.Dequeue(&tmp);
    buffers_.clear();
    delete log_writer_;
  }

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
  friend class LogWriter;
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
  // This stores all the buffers the serializer or the log writer threads use
  std::vector<BufferedLogWriter> buffers_;
  // This is the buffer the serializer thread will write to
  BufferedLogWriter *buffer_to_write_;
  // The queue containing empty buffers which the serializer thread will use
  common::ConcurrentBlockingQueue<BufferedLogWriter *> empty_buffer_queue_;
  // The queue containing filled buffers pending flush to the disk
  common::ConcurrentBlockingQueue<BufferedLogWriter *> filled_buffer_queue_;

  // The log writer object which flushes filled buffers to the disk
  LogWriter *log_writer_;
  // Flag used by the serializer thread to signal shutdown to the log writer thread
  volatile bool run_log_writer_thread_;
  // Flag used by the serializer thread to signal the log writer thread to persist the data on disk
  volatile bool do_persist_;

  // Synchronisation primitives to synchronise persisting buffers to disk
  std::mutex persist_lock_;
  std::condition_variable persist_cv_;
  std::condition_variable persist_and_empty_queue_cv_;

  /**
   * Serialize out the record to the log
   * @param record the redo record to serialise
   */
  void SerializeRecord(const LogRecord &record);

  /**
   * Serialize out the task buffer to the log
   * @param task_buffer the iterator to the redo buffer to be serialized
   */
  void SerializeTaskBuffer(IterableBufferSegment<LogRecord> *task_buffer);

  /**
   * Used by the serializer thread to get a buffer to serialize data to
   * @return buffer to write to
   */
  BufferedLogWriter *GetBufferToWrite() {
    if (buffer_to_write_ == nullptr) {
      empty_buffer_queue_.Dequeue(&buffer_to_write_);
    }
    return buffer_to_write_;
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
  void MarkBufferFull() {
    filled_buffer_queue_.Enqueue(buffer_to_write_);
    persist_and_empty_queue_cv_.notify_one();
    buffer_to_write_ = nullptr;
  }
};
}  // namespace terrier::storage
