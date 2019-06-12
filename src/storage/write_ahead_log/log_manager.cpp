#include "storage/write_ahead_log/log_manager.h"
#include "storage/write_ahead_log/log_serializer_task.h"
#include "transaction/transaction_context.h"

namespace terrier::storage {

void LogManager::Start() {
  TERRIER_ASSERT(!run_log_manager_, "Can't call Start on already started LogManager");
  // Initialize buffers for logging
  for (size_t i = 0; i < num_buffers_; i++) {
    buffers_.emplace_back(BufferedLogWriter(log_file_path_.c_str()));
  }
  for (size_t i = 0; i < num_buffers_; i++) {
    empty_buffer_queue_.Enqueue(&buffers_[i]);
  }

  run_log_manager_ = true;

  // Register DiskLogConsumerTask
  disk_log_writer_task_ = DedicatedThreadRegistry::GetInstance().RegisterDedicatedThread<DiskLogConsumerTask>(
      this /* requester */, &buffers_, &empty_buffer_queue_, &filled_buffer_queue_);

  // Register LogFlusherTask
  log_flusher_task_ = DedicatedThreadRegistry::GetInstance().RegisterDedicatedThread<LogFlusherTask>(
      this /* requester */, this /* argument to task constructor */, flushing_interval_);

  // Register LogSerializerTask
  log_serializer_task_ = DedicatedThreadRegistry::GetInstance().RegisterDedicatedThread<LogSerializerTask>(
      this /* requester */, serialization_interval_, buffer_pool_, &empty_buffer_queue_, &filled_buffer_queue_,
      &disk_log_writer_task_->disk_log_writer_thread_cv_);
}

void LogManager::ForceFlush() {
  std::unique_lock<std::mutex> lock(disk_log_writer_task_->persist_lock_);
  // Signal the disk log consumer task thread to persist the buffers to disk
  disk_log_writer_task_->do_persist_ = true;
  disk_log_writer_task_->disk_log_writer_thread_cv_.notify_one();

  // Wait for the disk log consumer task thread to persist the logs
  disk_log_writer_task_->persist_cv_.wait(lock, [&] { return !disk_log_writer_task_->do_persist_; });
}

void LogManager::PersistAndStop() {
  TERRIER_ASSERT(run_log_manager_, "Can't call PersistAndStop on an un-started LogManager");
  run_log_manager_ = false;

  // Signal all tasks to stop. The shutdown of the tasks will trigger a process and flush. The order in which we do
  // these is important, we must first serialize, then flush, then shutdown the disk consumer task
  auto result UNUSED_ATTRIBUTE = DedicatedThreadRegistry::GetInstance().StopTask(
      this, log_serializer_task_.CastManagedPointerTo<DedicatedThreadTask>());
  TERRIER_ASSERT(result, "LogFlusherTask should have been stopped");
  result = DedicatedThreadRegistry::GetInstance().StopTask(
      this, log_flusher_task_.CastManagedPointerTo<DedicatedThreadTask>());
  TERRIER_ASSERT(result, "DiskLogWriterTask should have been stopped");

  result = DedicatedThreadRegistry::GetInstance().StopTask(
      this, disk_log_writer_task_.CastManagedPointerTo<DedicatedThreadTask>());
  TERRIER_ASSERT(result, "LogSerializerTask should have been stopped");
  TERRIER_ASSERT(filled_buffer_queue_.Empty(), "disk log consumer task should have processed all filled buffers\n");

  // Close the buffers corresponding to the log file
  for (auto buf : buffers_) {
    buf.Close();
  }
  // Clear buffer queues
  empty_buffer_queue_.Clear();
  filled_buffer_queue_.Clear();
  buffers_.clear();
}

void LogManager::AddBufferToFlushQueue(RecordBufferSegment *const buffer_segment) {
  TERRIER_ASSERT(run_log_manager_, "Must call Start on log manager before handing it buffers");
  log_serializer_task_->AddBufferToFlushQueue(buffer_segment);
}

}  // namespace terrier::storage
