#pragma once
#include <condition_variable>  // NOLINT
#include <functional>
#include <mutex>  // NOLINT
#include <queue>
#include <random>
#include <thread>  // NOLINT
#include <utility>
#include <vector>

#include "common/object_pool.h"
#include "common/worker_pool.h"
#include "gtest/gtest.h"

namespace terrier {
/**
 * Thread pool for use in tests to avoid creating and destroying a ton of threads when running multiple iterations
 */
struct MultiThreadTestUtil {
  MultiThreadTestUtil() = delete;
  /**
   * Execute the workload with the specified number of threads and wait for them to finish before
   * returning. This can be done repeatedly if desired. Threads will be reused.
   *
   * @param thread_pool the thread pool for running workloads. It should be a clean thread_pool where no tasks
   * has been assigned yet.
   * @param num_threads number of threads to use for execution
   * @param workload the task the thread should run
   * @param repeat the number of times this should be done.
   */
  static void RunThreadsUntilFinish(common::WorkerPool *thread_pool, uint32_t num_threads,
                                    const std::function<void(uint32_t)> &workload, uint32_t repeat = 1) {
    // Shut the thread_pool down to set the number of threads
    thread_pool->Shutdown();
    thread_pool->SetNumWorkers(num_threads);
    for (uint32_t i = 0; i < repeat; i++) {
      thread_pool->Startup();
      // add the jobs to the queue
      for (uint32_t j = 0; j < thread_pool->NumWorkers(); j++) {
        thread_pool->SubmitTask([j, &workload] { workload(j); });
      }
      thread_pool->WaitUntilAllFinished();
      thread_pool->Shutdown();
    }
  }

  /**
   * @return the number of concurrent threads supported on this machine, or 0 if not well-defined.
   */
  static uint32_t HardwareConcurrency() noexcept { return std::thread::hardware_concurrency(); }
};

}  // namespace terrier
