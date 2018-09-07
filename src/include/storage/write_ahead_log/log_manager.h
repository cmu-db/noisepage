#pragma once

#include <functional>
#include <string>
#include <unordered_map>
#include <vector>
#include "common/container/concurrent_queue.h"
#include "common/spin_latch.h"
#include "common/typedefs.h"
#include "storage/record_buffer.h"
#include "storage/write_ahead_log/log_io.h"
#include "storage/write_ahead_log/log_record.h"

namespace terrier::storage {
/**
 * A LogManager is responsible for serializing log records out and keeping track of whether changes from a transaction
 * are persistent.
 */
class LogManager {
 public:
  /**
   * Constructs a new LogManager, writing its logs out to the given file.
   * @param log_file_path path to the desired log file location. If the log file does not exist, one will be created;
   *                      otherwise, changes are appended to the end of the file.
   * @param buffer_pool the object pool to draw log buffers from.
   */
  LogManager(const char *log_file_path, RecordBufferSegmentPool *buffer_pool)
      : out_(log_file_path), buffer_pool_(buffer_pool) {}

  /**
   * Requests a new log buffer for redo buffers, drawn from the LogManager's object pool. This method can be called
   * safely from concurrent execution threads.
   * @return a new log buffer for redo buffers.
   */
  BufferSegment *NewLogBuffer() { return buffer_pool_->Get(); }

  /**
   * Returns a (perhaps partially) filled log buffer to the log manager to be consumed. Caller should drop its
   * reference to the buffer after the method returns immediately, as it would no longer be safe to read from or
   * write to the buffer. This method can be called safely from concurrent execution threads.
   * @param buffer the (perhaps partially) filled log buffer ready to be consumed
   */
  void AddBufferToFlushQueue(BufferSegment *buffer) { flush_queue_.Enqueue(buffer); }

  /**
   * Register a callback for the committed transaction beginning at the given time, such that the callback will be
   * called as soon as possible by the LogManager when its commit record is out. Behavior is not defined if the provided
   * transaction is aborted or uncommitted and the callback might never be invoked.
   *
   * @param txn_begin begin timestamp of the transaction to observe on
   * @param callback the callback to invoke when the commit record is persistent; this callback will be invoked on the
   *                 serializing thread, and should be kept short and fast, performing no expensive computation or waits
   *                 on resources.
   */
  void RegisterTransactionFlushedCallback(timestamp_t txn_begin, const std::function<void()> &callback) {
    common::SpinLatch::ScopedSpinLatch guard(&callbacks_latch_);
    auto ret UNUSED_ATTRIBUTE = callbacks_.emplace(txn_begin, callback);
    TERRIER_ASSERT(ret.second, "Insertion failed, callback is already registered for given transaction");
  }

  /**
   * Process all the accumulated log records and serialize them out to disk. Flush can happen immediately or later
   * depending on the state of the LogManager, and an explicit call to Flush() is required for any guarantee. (Beware
   * the performance consequences of calling flush too frequently) This method should only be called from a dedicated
   * logging thread.
   */
  void Process() {
    BufferSegment *buffer;
    while (flush_queue_.Dequeue(&buffer)) {
      for (LogRecord &record : IterableBufferSegment<LogRecord>(buffer)) {
        SerializeRecord(record);
        if (record.RecordType() == LogRecordType::COMMIT) commits_in_buffer_.push_back(record.TxnBegin());
      }
      buffer_pool_->Release(buffer);
    }
  }

  /**
   * Flush the logs to make sure all serialized records before this invocation are persistent. Callbacks from committed
   * transactions are also invoked when possible. This method should only be called from a dedicated logging thread.
   */
  void Flush() {
    out_.Flush();
    common::SpinLatch::ScopedSpinLatch guard(&callbacks_latch_);
    for (timestamp_t txn : commits_in_buffer_) {
      auto it = callbacks_.find(txn);
      TERRIER_ASSERT(it != callbacks_.end(), "committing transaction does not have a registered callback for flush");
      it->second();
      callbacks_.erase(it);
    }
    commits_in_buffer_.clear();
  }

 private:
  // TODO(Tianyu): This can be changed later to be include things that are not necessarily backed by a disk
  // (e.g. logs can be streamed out to the network for remote replication)
  BufferedLogWriter out_;
  RecordBufferSegmentPool *buffer_pool_;

  // These need to be thread-safe since various execution threads will modify these
  common::ConcurrentQueue<BufferSegment *> flush_queue_;

  // TODO(Tianyu): Might not be necessary, since commit on txn manager is already protected with a latch
  common::SpinLatch callbacks_latch_;
  std::unordered_map<timestamp_t, std::function<void()>> callbacks_;

  // These do not need to be thread safe since the only thread adding or removing from it is the flushing thread
  std::vector<timestamp_t> commits_in_buffer_;

  void SerializeRecord(const LogRecord &record) {
    WriteValue(record.Size());
    WriteValue(record.RecordType());
    WriteValue(record.TxnBegin());
    switch (record.RecordType()) {
      case LogRecordType::REDO: {
        auto *record_body = record.GetUnderlyingRecordBodyAs<RedoRecord>();
        WriteValue(record_body->GetDataTable()->TableOid());
        WriteValue(record_body->GetTupleSlot());
        // TODO(Tianyu): Need to inline varlen or other things, and figure out a better representation.
        Write(record_body->Delta(), record_body->Delta()->Size());
        break;
      }
      case LogRecordType::COMMIT:
        WriteValue(record.GetUnderlyingRecordBodyAs<CommitRecord>()->CommitTime());
    }
  }

  void Write(const void *data, uint32_t size) {
    if (!out_.CanBuffer(size)) Flush();
    if (!out_.CanBuffer(size)) {
      // This write is too large to fit into a buffer, we need to write directly without a buffer,
      // but no flush is necessary since the commit records are always small enough to be buffered
      out_.WriteUnsyncedFully(data, size);
      return;
    }
    // Write can be buffered
    out_.BufferWrite(data, size);
  }

  template <class T>
  void WriteValue(const T &val) {
    Write(&val, sizeof(T));
  }
};
}  // namespace terrier::storage
