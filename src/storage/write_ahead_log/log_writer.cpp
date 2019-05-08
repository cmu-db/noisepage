#include "storage/write_ahead_log/log_writer.h"
#include "storage/write_ahead_log/log_manager.h"

namespace terrier::storage {

void LogWriter::FlushAllBuffers() {
  // Persist all the filled buffers to the disk
  while (!log_manager_->filled_buffer_queue_.Empty()) {
    // Dequeue filled buffers and flush them to disk
    BufferedLogWriter *buf;
    log_manager_->filled_buffer_queue_.Dequeue(&buf);
    buf->FlushBuffer();
    // Enqueue the flushed buffer to the empty buffer queue
    log_manager_->empty_buffer_queue_.Enqueue(buf);
  }
  // Persist the buffers
  TERRIER_ASSERT(!(log_manager_->buffers_.empty()), "Buffers vector should not be empty until Shutdown");
  // Force the buffers to be written to disk
  log_manager_->buffers_.front().Persist();
}

void LogWriter::WriteToDisk() {
  // Log writer thread spins in this loop
  // It dequeues a filled buffer and flushes it to disk
  while (log_manager_->run_log_writer_thread_) {
    BufferedLogWriter *buf;
    {
      std::unique_lock<std::mutex> lock(log_manager_->persist_lock_);
      // Wake up the writer thread if:
      // 1) The serializer thread has signalled to persist all non-empty buffers to disk
      // 2) There is a filled buffer to write to the disk
      // 3) Logging shutdown has initiated
      log_manager_->wake_writer_thread_cv_.wait(lock, [&] {
        return log_manager_->do_persist_ || !log_manager_->filled_buffer_queue_.Empty() ||
               !log_manager_->run_log_writer_thread_;
      });
    }
    // Flush all the filled buffers
    while (!log_manager_->filled_buffer_queue_.Empty()) {
      log_manager_->filled_buffer_queue_.Dequeue(&buf);
      // Flush the buffer to the disk
      buf->FlushBuffer();
      // Push the emptied buffer to queue of available buffers to fill
      log_manager_->empty_buffer_queue_.Enqueue(buf);
    }
    // If the main logger thread has signaled to persist the buffers, persist all the filled buffers
    if (log_manager_->do_persist_) {
      FlushAllBuffers();
      // Signal the main logger thread for completion of persistence
      {
        std::unique_lock<std::mutex> lock(log_manager_->persist_lock_);
        log_manager_->do_persist_ = false;
      }
      // Signal the serialiser thread that persist is over
      log_manager_->persist_cv_.notify_one();
    }
  }
}
}  // namespace terrier::storage
