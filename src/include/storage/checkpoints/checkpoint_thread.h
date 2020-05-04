#include <chrono>
#include <condition_variable>
#include <iostream>
#include <mutex>
#include <cstdint>
#include <thread>

#include "storage/checkpoints/checkpoint.h"

#ifndef TERRIER_CHECKPOINT_THREAD_H
#define TERRIER_CHECKPOINT_THREAD_H
namespace terrier::storage {
  class CheckpointBackgroundLoop {
    static std::condition_variable cv;
    static std::mutex mut;
    static bool stop = false;
    static Checkpoint checkpoint;
    static const catalog::db_oid_t db;

    static void BackgroundLoop(const int64_t epoch) {
      using delta = std::chrono::duration<std::int64_t, std::ratio<1, 60>>;
      auto next = std::chrono::steady_clock::now() + delta{1};
      std::unique_lock<std::mutex> lk(mut);
      while (!stop)
      {
        mut.unlock();
        // Do stuff
        std::cerr << "working...\n";
        checkpoint.TakeCheckpoint("ckpt_test/", db, "");
        // Wait for the next 1/60 sec
        mut.lock();
        cv.wait_until(lk, next, []{return false;});
        next += delta{epoch};
      }
    }

    // Epoch is number of seconds to wait
    static void StartBackgroundLoop(const int64_t epoch) {
      using namespace std::chrono_literals;
      std::thread t{BackgroundLoop, epoch};
      // Duration
      std::this_thread::sleep_for(5s);
      {
        std::lock_guard<std::mutex> lk(mut);
        stop = true;
      }
      t.join();
    }
  };

}


#endif //TERRIER_CHECKPOINT_THREAD_H
