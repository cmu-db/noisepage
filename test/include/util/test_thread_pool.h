#pragma once
#include <condition_variable>  // NOLINT
#include <functional>
#include <mutex>  // NOLINT
#include <queue>
#include <random>
#include <thread>  // NOLINT
#include <utility>
#include <vector>
#include "common/container/concurrent_vector.h"
#include "common/object_pool.h"
#include "common/worker_pool.h"
#include "gtest/gtest.h"

namespace terrier {
/**
 * Thread pool for use in tests to avoid creating and destroying a ton of threads when running multiple iterations
 */
class TestThreadPool {
 public:
  /**
   * Execute the workload with the specified number of threads and wait for them to finish before
   * returning. This can be done repeatedly if desired. Threads will be reused.
   *
   * @param num_threads number of threads to use for execution
   * @param workload the task the thread should run
   * @param repeat the number of times this should be done.
   */
  void RunThreadsUntilFinish(uint32_t num_threads, const std::function<void(uint32_t)> &workload, uint32_t repeat = 1) {
    common::WorkerPool thread_pool(num_threads, {});
    for (uint32_t i = 0; i < repeat; i++) {
      // add the jobs to the queue
      for (uint32_t j = 0; j < num_threads; j++) {
        thread_pool.SubmitTask([j, &workload] { workload(j); });
      }
      thread_pool.WaitUtilFinish();
    }
    thread_pool.Shutdown();
  }

  /**
   * @return the number of concurrent threads supported on this machine, or 0 if not well-defined.
   */
  static uint32_t HardwareConcurrency() noexcept { return std::thread::hardware_concurrency(); }
};

}  // namespace terrier
