#pragma once
#include <atomic>
#include <condition_variable>  // NOLINT
#include <functional>
#include <iostream>
#include <mutex>  // NOLINT
#include <queue>
#include <string>
#include <thread>  // NOLINT
#include <utility>
#include <vector>

namespace terrier::common {

using TaskQueue = std::queue<std::function<void()>>;

/**
 * @brief A worker pool that maintains a group of worker threads. This pool is
 * restartable, meaning it can be started again after it has been shutdown.
 * Calls to Startup() and Shutdown() are thread-safe and idempotent.
 */
class WorkerPool {
 public:
  /**
   * Initialize the worker pool.
   *
   * @param pool_name the name of the worker pool
   * @param num_workers the number of workers in this pool
   * @param task_queue a queue of tasks
   */
  WorkerPool(std::string pool_name, uint32_t num_workers, TaskQueue task_queue)
      : pool_name_(std::move(pool_name)),
        num_workers_(num_workers),
        is_running_(false),
        task_queue_(std::move(task_queue)) {}

  /**
   * Destructor. Wake up all workers and let them finish before it's destroyed.
   */
  ~WorkerPool() {
    std::unique_lock<std::mutex> lock(task_lock_);  // grab the lock
    is_running_ = false;                            // signal all the threads to shutdown
    task_cv_.notify_all();                          // wake up all the threads
    lock.unlock();                                  // free the lock
    for (auto &thread : workers_) thread.join();
  }

  /**
   * @brief Start this worker pool. Thread-safe and idempotent.
   */
  void Startup() {
    is_running_ = true;
    while (workers_.size() < num_workers_) {
      AddThread();
    }
  }

  /**
   * @brief Shutdown this worker pool. Thread-safe and idempotent.
   */
  void Shutdown() {
    is_running_ = false;
    std::unique_lock<std::mutex> lock(task_lock_);
    // wait for all the threads to finish
    finished_cv_.wait(lock, [this] { return busy_workers_ == 0; });
  }

  template <typename F>
  inline void SubmitTask(const F &func) {
    if (!is_running_) {
      Startup();
    }
    std::unique_lock<std::mutex> lock(task_lock_);  // grab the lock
    task_queue_.emplace(std::move(func));
    task_cv_.notify_one();
  }
  /**
   * @brief Access the number of worker threads in this pool
   *
   * @return The number of worker threads assigned to this pool
   */
  uint32_t NumWorkers() const { return num_workers_; }

 private:
  // The name of this pool
  std::string pool_name_;
  // The worker threads
  std::vector<std::thread> workers_;
  // The number of worker threads
  uint32_t num_workers_;
  // Flag indicating whether the pool is running
  std::atomic_bool is_running_;
  // The queue where workers pick up tasks
  TaskQueue task_queue_;

  uint32_t busy_workers_ = 0;

  std::mutex task_lock_;

  std::condition_variable task_cv_;

  std::condition_variable finished_cv_;

  void AddThread() {
    workers_.emplace_back([this] {
      // keep the thread alive
      while (true) {
        // grab the lock
        std::unique_lock<std::mutex> lock(task_lock_);
        // try to get work
        task_cv_.wait(lock, [this] { return !is_running_ || !task_queue_.empty(); });
        // woke up! time to work or time to die?
        if (!is_running_) {
          break;
        }
        // grab the work
        ++busy_workers_;
        auto task = std::move(task_queue_.front());
        task_queue_.pop();
        // release the lock while we work
        lock.unlock();
        task();
        // we lock again to notify that we're done
        lock.lock();
        --busy_workers_;
        finished_cv_.notify_one();
      }
    });
  }
};
}  // namespace terrier::common
