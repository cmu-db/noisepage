#pragma once

#include <queue>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include "common/spin_latch.h"
#include "common/strong_typedef.h"
#include "spdlog/details/mpmc_blocking_q.h"
#include "storage/record_buffer.h"
#include "storage/write_ahead_log/log_io.h"
#include "storage/write_ahead_log/log_record.h"
#include "transaction/transaction_defs.h"

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
        buffer_to_write_(nullptr),
        empty_buffer_queue_(MAX_BUF),
        filled_buffer_queue_(MAX_BUF),
        run_log_writer_thread_(true),
        do_persist_(true) {
    for (int i = 0; i < MAX_BUF; i++) {
      buffers_.emplace_back(BufferedLogWriter(log_file_path));
    }
    for (int i = 0; i < MAX_BUF; i++) {
      BlockingEnqueueBuffer(&buffers_[i], &empty_buffer_queue_);
    }
    log_writer_thread_ = std::thread([this] { WriteToDiskLoop(); });
  }

  /**
   * Must be called when no other threads are doing work
   */
  void Shutdown() {
    Process();
    run_log_writer_thread_ = false;
    log_writer_thread_.join();
    for (auto buf : buffers_) {
      buf.Close();
    }
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

  using queue_t = spdlog::details::mpmc_blocking_queue<BufferedLogWriter *>;

  // This stores all the buffers the serializer or the log writer threads use
  std::vector<BufferedLogWriter> buffers_;
  // This is the buffer the serializer thread will write to
  BufferedLogWriter *buffer_to_write_;
  // The queue containing empty buffers which the serializer thread will use
  queue_t empty_buffer_queue_;
  // The queue containing filled buffers pending flush to the disk
  queue_t filled_buffer_queue_;

  // The log writer thread which flushes filled buffers to the disk
  std::thread log_writer_thread_;
  // Flag used by the serializer thread to signal shutdown to the log writer thread
  volatile bool run_log_writer_thread_;
  // Flag used by the serializer thread to signal the log writer thread to persist the data on disk
  volatile bool do_persist_;

  /**
   * Serialize out the record to the log
   * @param task_buffer the task buffer
   */
  void SerializeRecord(const LogRecord &record);

  /**
   * Serialize out the task buffer to the log
   * @param task_buffer the task buffer
   */
  void SerializeTaskBuffer(IterableBufferSegment<LogRecord> *task_buffer);

  /**
   * Write data to disk till shutdown. This is what the log writer thread runs
   */
  void WriteToDiskLoop();

  /**
   * Flush all buffers in the filled buffers queue to the disk, followed by an fsync
   */
  void FlushAllBuffers();

  /**
   * Used by the serializer thread to get a buffer to serialize data to
   * @return buffer to write to
   */
  BufferedLogWriter *GetBufferToWrite() {
    if (buffer_to_write_ == nullptr) {
      buffer_to_write_ = BlockingDequeueBuffer(&empty_buffer_queue_);
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
   * Enqueue a buffer to the queue
   * Blocks if the queue is full
   * @param queue the queue to get the buffer from
   */
  void BlockingEnqueueBuffer(BufferedLogWriter *buf, queue_t *queue) { queue->enqueue(&(*buf)); }

  /**
   * Dequeue a buffer from queue and return it
   * Blocks if the queue is empty
   * @param queue the queue to get the buffer from
   * @return a buffer
   */
  BufferedLogWriter *BlockingDequeueBuffer(queue_t *queue) {
    bool dequeued = false;
    BufferedLogWriter *buf = nullptr;
    while (!dequeued) {
      // Get a buffer from the queue of buffers pending write
      dequeued = queue->dequeue_for(buf, std::chrono::milliseconds(QUEUE_WAIT_TIME_MILLISECONDS));
    }
    return buf;
  }

  /**
   * Dequeue a buffer from queue and put it in buf
   * Blocks for a few milliseconds (@see QUEUE_WAIT_TIME_MILLISECONDS) if queue is empty
   * @param buf the buffer which was dequeued
   * @param queue the queue to get the buffer from
   * @return true if a buffer was dequeued
   */
  bool NonblockingDequeueBuffer(BufferedLogWriter **buf, queue_t *queue) {
    return queue->dequeue_for(*buf, std::chrono::milliseconds(QUEUE_WAIT_TIME_MILLISECONDS));
  }

  /**
   * Mark the current buffer that the serializer thread is writing to as filled
   */
  void MarkBufferFull() {
    if (buffer_pool_ != nullptr) {
      BlockingEnqueueBuffer(buffer_to_write_, &filled_buffer_queue_);
      buffer_to_write_ = nullptr;
    }
  }
};
}  // namespace terrier::storage
