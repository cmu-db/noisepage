#include "storage/write_ahead_log/log_manager.h"

#include "common/dedicated_thread_registry.h"
#include "storage/write_ahead_log/disk_log_consumer_task.h"
#include "storage/write_ahead_log/log_serializer_task.h"
#include "transaction/transaction_context.h"

namespace noisepage::storage {

void LogManager::Start() {
  NOISEPAGE_ASSERT(!run_log_manager_, "Can't call Start on already started LogManager");
  // Initialize buffers for logging
  for (size_t i = 0; i < num_buffers_; i++) {
    buffers_.emplace_back(BufferedLogWriter(log_file_path_.c_str()));
  }
  for (size_t i = 0; i < num_buffers_; i++) {
    empty_buffer_queue_.Enqueue(&buffers_[i]);
  }

  run_log_manager_ = true;

  // Register DiskLogConsumerTask
  disk_log_writer_task_ = thread_registry_->RegisterDedicatedThread<DiskLogConsumerTask>(
      this /* requester */, persist_interval_, persist_threshold_, &buffers_, &empty_buffer_queue_,
      &filled_buffer_queue_);

  // Register LogSerializerTask
  log_serializer_task_ = thread_registry_->RegisterDedicatedThread<LogSerializerTask>(
      this /* requester */, serialization_interval_, buffer_pool_, &empty_buffer_queue_, &filled_buffer_queue_,
      &disk_log_writer_task_->disk_log_writer_thread_cv_);
}

void LogManager::ForceFlush() {
  // Force the serializer task to serialize buffers
  log_serializer_task_->Process();
  // Signal the disk log consumer task thread to persist the buffers to disk
  std::unique_lock<std::mutex> lock(disk_log_writer_task_->persist_lock_);
  disk_log_writer_task_->force_flush_ = true;
  disk_log_writer_task_->disk_log_writer_thread_cv_.notify_one();

  // Wait for the disk log consumer task thread to persist the logs
  disk_log_writer_task_->persist_cv_.wait(lock, [&] { return !disk_log_writer_task_->force_flush_; });
}

void LogManager::PersistAndStop() {
  NOISEPAGE_ASSERT(run_log_manager_, "Can't call PersistAndStop on an un-started LogManager");
  run_log_manager_ = false;

  // Signal all tasks to stop. The shutdown of the tasks will trigger any remaining logs to be serialized, writen to the
  // log file, and persisted. The order in which we shut down the tasks is important, we must first serialize, then
  // shutdown the disk consumer task (reverse order of Start())
  auto result UNUSED_ATTRIBUTE =
      thread_registry_->StopTask(this, log_serializer_task_.CastManagedPointerTo<common::DedicatedThreadTask>());
  NOISEPAGE_ASSERT(result, "LogSerializerTask should have been stopped");

  result = thread_registry_->StopTask(this, disk_log_writer_task_.CastManagedPointerTo<common::DedicatedThreadTask>());
  NOISEPAGE_ASSERT(result, "DiskLogConsumerTask should have been stopped");
  NOISEPAGE_ASSERT(filled_buffer_queue_.Empty(), "disk log consumer task should have processed all filled buffers\n");

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
  NOISEPAGE_ASSERT(run_log_manager_, "Must call Start on log manager before handing it buffers");
  log_serializer_task_->AddBufferToFlushQueue(buffer_segment);
}

}  // namespace noisepage::storage
