#include "storage/write_ahead_log/disk_log_consumer_task.h"

namespace terrier::storage {

void DiskLogConsumerTask::RunTask() {
  run_task_ = true;
  DiskLogConsumerTaskLoop();
}

void DiskLogConsumerTask::Terminate() {
  // If the task hasn't run yet, yield the thread until it's started
  while (!run_task_) std::this_thread::yield();
  TERRIER_ASSERT(run_task_, "Cant terminate a task that isnt running");
  // Signal to terminate and force a flush so task persists before LogManager closes buffers
  run_task_ = false;
  disk_log_writer_thread_cv_.notify_one();
}

void DiskLogConsumerTask::WriteBuffersToLogFile() {
  // Persist all the filled buffers to the disk
  SerializedLogs logs;
  while (!filled_buffer_queue_->Empty()) {
    // Dequeue filled buffers and flush them to disk, as well as storing commit callbacks
    filled_buffer_queue_->Dequeue(&logs);
    logs.first->FlushBuffer();
    commit_callbacks_.insert(commit_callbacks_.end(), logs.second.begin(), logs.second.end());
    // Enqueue the flushed buffer to the empty buffer queue
    empty_buffer_queue_->Enqueue(logs.first);
  }
}

void DiskLogConsumerTask::PersistLogFile() {
  TERRIER_ASSERT(!buffers_->empty(), "Buffers vector should not be empty until Shutdown");
  // Force the buffers to be written to disk. Because all buffers log to the same file, it suffices to call persist on
  // any buffer.
  buffers_->front().Persist();
  // Execute the callbacks for the transactions that have been persisted
  for (auto &callback : commit_callbacks_) callback.first(callback.second);
  commit_callbacks_.clear();
}

void DiskLogConsumerTask::DiskLogConsumerTaskLoop() {
  // disk log consumer task thread spins in this loop
  // It dequeues a filled buffer and flushes it to disk
  do {
    bool timeout;
    {
      // Wait until we are told to flush buffers
      std::unique_lock<std::mutex> lock(persist_lock_);
      // Wake up the task thread if:
      // 1) The serializer thread has signalled to persist all non-empty buffers to disk
      // 2) There is a filled buffer to write to the disk
      // 3) LogManager has shut down the task
      // 4) Our persist interval timed out
      // TODO(Gus): Also persist if weve written a certain amount of data
      // TODO(Gus): Should we wake up EVERY time we receive a buffer?
      timeout = disk_log_writer_thread_cv_.wait_for(
          lock, persist_interval_, [&] { return do_persist_ || !filled_buffer_queue_->Empty() || !run_task_; });
    }

    // Flush all the buffers to the log file
    WriteBuffersToLogFile();

    // We persist the log file if the following conditions are met
    // 1) The persist interval amount of time has passed
    // 2) We are signaled to persist
    // 3) We are shutting down this task
    if (timeout || do_persist_ || !run_task_) {
      {
        std::unique_lock<std::mutex> lock(persist_lock_);
        PersistLogFile();
        do_persist_ = false;
      }
      // Signal anyone who forced a persist that the persist has finished
      persist_cv_.notify_all();
    }
  } while (run_task_);
}
}  // namespace terrier::storage
