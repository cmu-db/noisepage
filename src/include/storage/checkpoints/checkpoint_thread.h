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
  std::condition_variable cv;
  std::mutex mut;
  bool stop = false;
  Checkpoint checkpoint;
  const catalog::db_oid_t db;

  void BackgroundLoop(const int64_t epoch) {
    using delta = std::chrono::duration<std::int64_t, std::ratio<1, 60>>;
    auto next = std::chrono::steady_clock::now() + delta{1};
    std::unique_lock<std::mutex> lk(mut);
    while (!stop) {
      mut.unlock();
      // Do stuff
      std::cerr << "working...\n";
      checkpoint.TakeCheckpoint("ckpt_test/", db, std::to_string(epoch).c_str());
      // Wait for the next 1/60 sec
      mut.lock();
      cv.wait_until(lk, next, [] { return false; });
      next += delta{epoch};
    }
  }

  // Epoch is number of seconds to wait
  void StartBackgroundLoop(const int64_t epoch) {
    using namespace std::chrono_literals;
    std::thread t(&CheckpointBackgroundLoop::BackgroundLoop, this, epoch);
    // Duration
    std::this_thread::sleep_for(5s);
    {
      std::lock_guard<std::mutex> lk(mut);
      stop = true;
    }
    t.join();
  }
};

}  // namespace terrier::storage
