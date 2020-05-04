#pragma once

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <iostream>
#include <mutex>
#include <thread>

#include "storage/checkpoints/checkpoint.h"

namespace terrier::storage {
class CheckpointBackgroundLoop {

  explicit CheckpointBackgroundLoop(const std::string &path, catalog::db_oid_t db, const char *cur_log_file, uint32_t num_threads, common::WorkerPool *thread_pool, Checkpoint* checkpoint)
                           : path_(path),
                            db_(db),
                            cur_log_file_(cur_log_file),
                            num_threads_(num_threads),
                            thread_pool_(thread_pool),
                            checkpoint_(checkpoint){
  }

  void BackgroundLoop(const int64_t epoch) {
    using delta = std::chrono::duration<std::int64_t, std::ratio<1, 60>>;
    auto next = std::chrono::steady_clock::now() + delta{1};
    std::unique_lock<std::mutex> lk(mut);
    while (!stop) {
      mut.unlock();
      // Do stuff
      STORAGE_LOG_INFO("Taking Checkpoint AT ", epoch);
      checkpoint_->TakeCheckpoint(path_ + std::to_string(epoch).c_str(), db_, cur_log_file_, num_threads_, thread_pool_);
      // Wait for the next epoch/60 sec
      mut.lock();
      cv.wait_until(lk, next, [] { return false; });
      next += delta{epoch};
    }
  }

  // Epoch is number of seconds to wait
  void StartBackgroundLoop(const int64_t epoch, const int64_t duration) {
    std::thread t(&CheckpointBackgroundLoop::BackgroundLoop, this, epoch);
    // Duration
    std::this_thread::sleep_for(std::chrono::seconds(duration));
    {
      std::lock_guard<std::mutex> lk(mut);
      stop = true;
    }
    t.join();
  }

private:
  const std::string path_;
  const catalog::db_oid_t  db_;
  const char* cur_log_file_;
  const uint32_t num_threads_;
  common::WorkerPool *thread_pool_;
  std::condition_variable cv;
  std::mutex mut;
  bool stop = false;
  Checkpoint *checkpoint_;
};

}  // namespace terrier::storage
