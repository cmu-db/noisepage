#pragma once
#include <fstream>
#include <unordered_map>
#include <functional>
#include "common/spin_latch.h"
#include "common/typedefs.h"
#include "common/container/concurrent_queue.h"
#include "storage/record_buffer.h"
#include "log_record.h"

namespace terrier::storage {
class LogManager {
 public:
  LogManager(const std::string &log_file_name, RecordBufferSegmentPool *buffer_pool)
      : out_(log_file_name, std::ios::binary | std::ios::app | std::ios::out),
        buffer_pool_(buffer_pool),
        flush_buffer_(buffer_pool_->Get()) {}

  ~LogManager() {
    out_.close();
    buffer_pool_->Release(flush_buffer_);
  }

  // The following should only be called on worked threads
  BufferSegment *NewLogBuffer() {
    return buffer_pool_->Get();
  }

  void AddBufferToFlushQueue(BufferSegment *buffer) {
    flush_queue_.Enqueue(buffer);
  }

  void OnTransactionFlushed(timestamp_t txn_begin, const std::function<void()> &callback) {
    common::SpinLatch::ScopedSpinLatch guard(&callbacks_latch_);
    auto ret UNUSED_ATTRIBUTE = callbacks_.emplace(txn_begin, callback);
    TERRIER_ASSERT(ret.second, "Insertion failed, callback is already registered for given transaction");
  }

  // The following should only be called by flushing thread
  void Process() {
    BufferSegment *next;
    while (flush_queue_.Dequeue(&next)) SerializeSegment(next);
  }

  void Write(const void *data, uint32_t size) {
    if (!flush_buffer_->HasBytesLeft(size)) {
      Flush();
      if (!flush_buffer_->HasBytesLeft(size)) {
        // This write is too large to fit into a buffer, we need to write directly without a buffer,
        // but no flush is necessary since the commit records are always small enough to be buffered
        out_.write(reinterpret_cast<const char *>(data), size);
        return;
      }
    }
    // Write can be buffered
    TERRIER_MEMCPY(flush_buffer_->Reserve(size), data, size);
  }

  template<class T>
  void WriteValue(const T &val) {
    Write(&val, sizeof(T));
  }

  void Flush() {
    out_.write(flush_buffer_->WritableHead(), flush_buffer_->Size());
    out_.flush();
    common::SpinLatch::ScopedSpinLatch guard(&callbacks_latch_);
//    for (timestamp_t txn : commits_in_buffer_) {
//      auto it = callbacks_.find(txn);
//       TODO(Tianyu): Is this too strict?
//      TERRIER_ASSERT(it != callbacks_.end(), "committing transaction does not have a registered callback for flush");
//      it->second();
//      callbacks_.erase(it);
//    }
    commits_in_buffer_.clear();
    flush_buffer_->Reset();
  }

 private:
  std::fstream out_;
  RecordBufferSegmentPool *buffer_pool_;

  // These need to be thread-safe since various execution threads will modify these
  common::ConcurrentQueue<BufferSegment *> flush_queue_;
  // TODO(Tianyu): Might not be necessary, since commit on txn manager is already protected with a latch
  common::SpinLatch callbacks_latch_;
  std::unordered_map<timestamp_t, const std::function<void()> &> callbacks_;

  // These do not need to be thread safe since the only thread adding or removing from it is the flushing thread
  std::vector<timestamp_t> commits_in_buffer_;
  BufferSegment *flush_buffer_;

  void SerializeSegment(BufferSegment *redo_buffer) {
    for (LogRecord &record : IterableBufferSegment<LogRecord>(redo_buffer)) {
      switch (record.RecordType()) {
        case LogRecordType::REDO:
          SerializeRecord(reinterpret_cast<RedoRecord &>(record));
          break;
        case LogRecordType::COMMIT:
          SerializeRecord(reinterpret_cast<CommitRecord &>(record));
          commits_in_buffer_.push_back(record.TxnBegin());
      }
    }
  }

  template <class RecordType>
  void SerializeRecord(const RecordType &record) {
    WriteValue(record.Size());
    record.SerializeToLog(this);
  }
};
}  // namespace terrier::storage