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
#include "common/macros.h"

namespace terrier::common {

using TaskQueue = std::queue<std::function<void()>>;

/**
 * A worker pool that maintains a group of worker threads and a task queue.
 *
 * As soon as there is a task in the task queue, a worker thread will be
 * assigned to run that task. A task must be a function that takes no argument.
 * After a worker finishes a task, it will eagerly try to get a new task.
 *
 * This pool is restartable, meaning it can be started again after it has been
 * shutdown. Calls to Startup() and Shutdown() are thread-safe.
 */
class WorkerPool {
 public:
  /**
   * Initialize the worker pool. Once the number of worker is set by the constructor
   * it cannot be changed.
   *
   * After initialization, the worker pool automatically starts up.
   *
   * @param num_workers the number of workers in this pool
   * @param task_queue a queue of tasks
   */
  // NOLINTNEXTLINE  lint thinks it has only one arguement
  WorkerPool(uint32_t num_workers = 1, TaskQueue task_queue = TaskQueue())
      : num_workers_(num_workers), is_running_(false), task_queue_(std::move(task_queue)), busy_workers_{0} {
    Startup();
  }

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
   * Start the worker pool. If the task queue is empty or we run out of tasks,
   * workers will be put into sleep.
   */
  void Startup() {
    is_running_ = true;
    while (workers_.size() < num_workers_) {
      AddThread();
    }
  }

  /**
   * It tells all worker threads to finish their current task and stop working.
   * No more tasks will be consumed. It waits until all worker threads stop working.
   */
  void Shutdown() {
    is_running_ = false;
    std::unique_lock<std::mutex> lock(task_lock_);
    // wait for all the threads to finish
    finished_cv_.wait(lock, [this] { return busy_workers_ == 0; });
  }

  /**
   * Add a task to the task queue and inform worker threads.
   * You can only submit tasks after the thread pool has started up.
   *
   * @param func the new task
   */
  template <typename F>
  void SubmitTask(const F &func) {
    TERRIER_ASSERT(is_running_, "Only allow to submit task after the thread pool has been started up");
    std::unique_lock<std::mutex> lock(task_lock_);
    task_queue_.emplace(std::move(func));
    task_cv_.notify_one();
  }

  /**
   * Block until all the tasks in the task queue has been completed
   */
  void WaitUntilAllFinished() {
    std::unique_lock<std::mutex> lock(task_lock_);
    // wait for all the tasks to complete
    finished_cv_.wait(lock, [this] { return busy_workers_ == 0 && task_queue_.empty(); });
  }

  /**
   * Get the number of worker threads in this pool
   *
   * @return The number of worker threads
   */
  uint32_t NumWorkers() const { return num_workers_; }

  /**
   * Change the number of worker threads. It can only be done when the thread pool
   * if not running.
   *
   * @param num the number of worker threads.
   */
  void SetNumWorkers(uint32_t num) {
    TERRIER_ASSERT(!is_running_, "Only allow to set num of workers when the thread pool is not running");
    if (num > num_workers_) {
      for (uint32_t i = 0; i < num - num_workers_; i++) {
        AddThread();
      }
    } else {
      workers_.resize(num);
    }
    num_workers_ = num;
  }

 private:
  // The worker threads
  std::vector<std::thread> workers_;
  // The number of worker threads
  uint32_t num_workers_;
  // Flag indicating whether the pool is running
  std::atomic_bool is_running_;
  // The queue where workers pick up tasks
  TaskQueue task_queue_;

  uint32_t busy_workers_;

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
        // We use notify_one because only the main thread(worker pool)
        // needs to need when a thread is finished.
        finished_cv_.notify_one();
      }
    });
  }
};
}  // namespace terrier::common
