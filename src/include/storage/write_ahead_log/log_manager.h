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
      : buffers_(MAX_BUF, BufferedLogWriter(log_file_path)),
        buffer_to_write_(nullptr),
        buffer_pool_(buffer_pool),
        empty_buffer_queue_(MAX_BUF),
        filled_buffer_queue_(MAX_BUF),
        run_log_writer_thread_(true),
        do_persist_(true) {
    log_writer_thread_ = std::thread([this] { WriteToDisk(); });
    for (int i = 0; i < MAX_BUF; i++) {
      empty_buffer_queue_.enqueue(&buffers_[i]);
    }
  }

  /**
   * Must be called when no other threads are doing work
   */
  void Shutdown() {
    Process();
    run_log_writer_thread_ = false;
    log_writer_thread_.join();
    buffers_[0].Close();
    buffers_.clear();
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
  std::vector<BufferedLogWriter> buffers_;

  BufferedLogWriter *buffer_to_write_;
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

  using item_type = BufferedLogWriter *;
  using q_type = spdlog::details::mpmc_blocking_queue<item_type>;

  q_type empty_buffer_queue_;
  q_type filled_buffer_queue_;

  std::thread log_writer_thread_;
  volatile bool run_log_writer_thread_;
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

  void WriteToDisk();
  void FlushAllBuffers();
  template <class T>
  void WriteValue(const T &val) {
    WriteValue(&val, sizeof(T));
  }

  BufferedLogWriter *BlockingDequeueBuffer(q_type *queue) {
    bool dequeued = false;
    BufferedLogWriter *buf = nullptr;
    while (!dequeued) {
      // Get a buffer from the queue of buffers pending write
      dequeued = queue->dequeue_for(buf, std::chrono::milliseconds(10));
    }
    return buf;
  }

  void BlockingEnqueueBuffer(BufferedLogWriter *buf, q_type *queue) { queue->enqueue(&(*buf)); }

  bool UnblockingDequeueBuffer(BufferedLogWriter **buf, q_type *queue) {
    return queue->dequeue_for(*buf, std::chrono::milliseconds(10));
  }

  BufferedLogWriter *GetBufferToWrite() {
    if (buffer_to_write_ == nullptr) {
      buffer_to_write_ = BlockingDequeueBuffer(&empty_buffer_queue_);
    }
    return buffer_to_write_;
  }

  void MarkBufferFull() {
    filled_buffer_queue_.enqueue(&(*buffer_to_write_));
    buffer_to_write_ = nullptr;
  }

  void WriteValue(const void *val, uint32_t size);
};
}  // namespace terrier::storage
