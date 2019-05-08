#include "storage/write_ahead_log/log_writer.h"
#include "storage/write_ahead_log/log_manager.h"

namespace terrier::storage {

void LogWriter::FlushAllBuffers() {
  // Persist all the filled buffers to the disk
  bool dequeued;
  do {
    // Dequeue filled buffers and flush them to disk
    BufferedLogWriter *buf;
    dequeued = log_manager_->filled_buffer_queue_.NonBlockingDequeue(&buf);
    if (dequeued) {
      buf->FlushBuffer();
      // Enqueue the flushed buffer to the empty buffer queue
      log_manager_->empty_buffer_queue_.BlockingEnqueue(buf);
    }
  } while (dequeued);
  // Persist the buffers
  TERRIER_ASSERT(!(log_manager_->buffers_.empty()), "Buffers vector should not be empty until Shutdown");
  log_manager_->buffers_.front().Persist();
}

void LogWriter::WriteToDisk() {
  // Log writer thread spins in this loop
  // It dequeues a filled buffer and flushes it to disk
  while (log_manager_->run_log_writer_thread_) {
    BufferedLogWriter *buf;
    bool dequeued = log_manager_->filled_buffer_queue_.NonBlockingDequeue(&buf);
    if (dequeued) {
      // Flush the buffer to the disk
      buf->FlushBuffer();
      // Push the emptied buffer to queue of available buffers to fill
      log_manager_->empty_buffer_queue_.BlockingEnqueue(buf);
    }
    // If the main logger thread has signaled to persist the buffers, persist all the filled buffers
    if (log_manager_->do_persist_) {
      FlushAllBuffers();
      // Signal the main logger thread for completion of persistence
      log_manager_->do_persist_ = false;
    }
  }
}
}  // namespace terrier::storage
