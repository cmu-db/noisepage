#pragma once

#include <atomic>
#include <chrono>              // NOLINT
#include <condition_variable>  // NOLINT
#include <cstdint>
#include <iostream>
#include <mutex>  // NOLINT
#include <string>
#include <thread>  // NOLINT

#include "storage/checkpoints/checkpoint.h"

namespace terrier::storage {
/**
 * A background loop that is responsible for taking checkpoints periodically.
 */
class CheckpointBackgroundLoop {
 public:
  /**
   * Maintains a system for taking periodic checkpoints. It first removes all preceding table
   * logs/checkpoints and then create a checkpoint with guaranteed persistency and consistency.
   * @param path to store the checkpoint
   * @param db database to take checkpoint of
   * @param cur_log_file provided log file path
   * @param num_threads number of threads used for thread pool
   * @param thread_pool thread pool used for taking checkpoint
   * @param checkpoint used for taking checkpoint
   */
  explicit CheckpointBackgroundLoop(const std::string &path, catalog::db_oid_t db, const char *cur_log_file,
                                    uint32_t num_threads, common::WorkerPool *thread_pool, Checkpoint *checkpoint)
      : path_(path),
        db_(db),
        cur_log_file_(cur_log_file),
        num_threads_(num_threads),
        thread_pool_(thread_pool),
        checkpoint_(checkpoint) {}

  /**
   * Background loop that takes checkpoint periodically
   * @param interval for taking checkpoint in seconds
   * @param num_checkpoints to take
   */
  void BackgroundLoop(const int64_t interval, const int64_t num_checkpoints) {
    std::unique_lock<std::mutex> lck(mtx);

    for (uint32_t i = 0; i < num_checkpoints; i++) {
      if (stop) break;
      STORAGE_LOG_INFO("Taking Checkpoint: ", i);
      checkpoint_->TakeCheckpoint(path_, db_, cur_log_file_, num_threads_, thread_pool_);
      STORAGE_LOG_INFO("Finish Checkpoint: ", i);
      cv.wait_for(lck, std::chrono::seconds(interval));
    }
  }

  /**
   * Initiates the background loop
   * @param interval for taking checkpoints
   * @param num_checkpoints to take
   */
  void StartBackgroundLoop(const int64_t interval, const int64_t num_checkpoints) {
    t = std::thread(&CheckpointBackgroundLoop::BackgroundLoop, this, interval, num_checkpoints);
  }

  /**
   * Stops the background loop
   */
  void EndBackgroundLoop() {
    stop = true;
    cv.notify_all();
    t.join();
  }

 private:
  const std::string path_;
  const catalog::db_oid_t db_;
  const char *cur_log_file_;
  const uint32_t num_threads_;
  common::WorkerPool *thread_pool_;
  Checkpoint *checkpoint_;
  std::atomic<bool> stop = false;
  std::thread t;

  std::mutex mtx;
  std::condition_variable cv;
};

}  // namespace terrier::storage
