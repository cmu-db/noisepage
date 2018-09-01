#pragma once
#include <fstream>
#include "common/typedefs.h"
#include "common/container/concurrent_queue.h"
#include "storage/record_buffer.h"

namespace terrier::storage {
class LogManager {
 public:
  LogManager(std::string log_file_name, RecordBufferSegmentPool *buffer_pool)
      : log_file_name_(std::move(log_file_name)), buffer_pool_(buffer_pool) {
//    log_io_.open(log_file_name_, std::ios::binary | std::ios::in | std::ios::app | std::ios::out);
  }

  BufferSegment *NewLogBuffer() {
    return buffer_pool_->Get();
  }

  void AddBufferToFlushQueue(BufferSegment *buffer) {

  }

  void FlushAll() {

  }

 private:
  std::string log_file_name_;
  std::fstream log_io_;
  common::ConcurrentQueue<BufferSegment *> flush_queue_;
  RecordBufferSegmentPool *buffer_pool_;
};
}  // namespace terrier::storage