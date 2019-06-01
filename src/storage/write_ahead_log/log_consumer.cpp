#include "storage/write_ahead_log/log_consumer.h"
#include "storage/write_ahead_log/log_manager.h"

namespace terrier::storage {

void LogConsumerTask::RunTask() {
  run_consumer_task_ = true;
  LogConsumerTaskLoop();
}

void LogConsumerTask::Terminate() {
  run_consumer_task_ = false;
  log_manager_->consumer_thread_cv_.notify_one();
}

void LogConsumerTask::FlushAllBuffers() {
  // Persist all the filled buffers to the disk
  BufferedLogWriter *buf;
  while (!log_manager_->filled_buffer_queue_.Empty()) {
    // Dequeue filled buffers and flush them to disk
    log_manager_->filled_buffer_queue_.Dequeue(&buf);
    buf->FlushBuffer();
    // Enqueue the flushed buffer to the empty buffer queue
    log_manager_->empty_buffer_queue_.Enqueue(buf);
  }
}

void LogConsumerTask::PersistAllBuffers() {
  TERRIER_ASSERT(!(log_manager_->buffers_.empty()), "Buffers vector should not be empty until Shutdown");
  // Force the buffers to be written to disk. Because all buffers log to the same file, it suffices to call persist on
  // any buffer.
  log_manager_->buffers_.front().Persist();
}

void LogConsumerTask::LogConsumerTaskLoop() {
  // Log Consumer thread spins in this loop
  // It dequeues a filled buffer and flushes it to disk
  while (run_consumer_task_) {
    // Wait until we are told to flush buffers
    {
      std::unique_lock<std::mutex> lock(log_manager_->persist_lock_);
      // Wake up the consumer thread if:
      // 1) The serializer thread has signalled to persist all non-empty buffers to disk
      // 2) There is a filled buffer to write to the disk
      // 3) LogManager has shut down the consumer task
      log_manager_->consumer_thread_cv_.wait(lock, [&] {
        return log_manager_->do_persist_ || !log_manager_->filled_buffer_queue_.Empty() || !run_consumer_task_;
      });
    }

    // Flush all the buffers
    FlushAllBuffers();

    // If the log manager has signaled to persist the buffers or the task is getting shut down, persist all the filled
    // buffers.
    if (log_manager_->do_persist_ || !run_consumer_task_) {
      // Signal the main logger thread for completion of persistence
      {
        std::unique_lock<std::mutex> lock(log_manager_->persist_lock_);
        PersistAllBuffers();
        log_manager_->do_persist_ = false;
      }
      // Signal the serializer thread that persist is over
      log_manager_->persist_cv_.notify_one();
    }
  }
}
}  // namespace terrier::storage
