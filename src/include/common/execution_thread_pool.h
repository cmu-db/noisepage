#pragma once

#define _GNU_SOURCE

#include <atomic>
#include <condition_variable>  // NOLINT
#include <functional>
#include <iostream>
#include <mutex>  // NOLINT
#include <queue>
#include <sched.h>
#include <string>
#include <thread>  // NOLINT
#include <utility>
#include <vector>
#include "common/macros.h"
#include "tbb/concurrent_queue.h"

namespace terrier::common {

enum class ThreadStatus = {
    FREE = 0,
    BUSY = 1,
    SWITCHING = 2,
    PARKED = 3
};

/**
 * A task queue is a FIFO list of functions that we will execute.
 * This queue by itself is not threadsafe so the WorkerPool class has to protect
 * it on its own with latches.
 */
// TODO(Deepayan): change from void later
using Task = std::function<void()>;
using TaskQueue = tbb::concurrent_queue<Task>;

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
class ExecutionThreadPool {
 public:
  // NOLINTNEXTLINE  lint thinks it has only one arguement
  ExecutionThreadPool(common::ManagedPointer<DedicatedThreadRegistry> thread_registry, std::vector<int> *cpu_ids)
      : thread_registry_(thread_registry), busy_workers_{0} {
    for (int cpu_id : cpu_id) {
      thread_registry_->RegisterDedicatedThread<TerrierThread>(this, cpu_id, this);
    }
  }

  /**
   * Destructor. Wake up all workers and let them finish before it's destroyed.
   */
  ~ExecutionThreadPool() {
    std::unique_lock<std::mutex> lock(task_lock_);  // grab the lock
    is_running_ = false;                            // signal all the threads to shutdown
    task_cv_.notify_all();                          // wake up all the threads
    lock.unlock();                                  // free the lock
    for (auto &thread : workers_) thread.join();
  }


  void SubmitTask(Task task, numa_region_t numa_hint = storage::UNSUPPORTED_NUMA_REGION) {
    if (numa_hint == storage::UNSUPPORTED_NUMA_REGION) {
      numa_hint = 0;
    }
    task_queue_[static_cast<int16_t>(numa_hint)].push(task);
    if (total_workers_ != busy_workers_) {
      task_cv_.notify_one();
    }
  }

  /**
   * Get the number of worker threads in this pool
   *
   * @return The number of worker threads
   */
  uint32_t NumWorkers() const { return num_workers_; }

  /*
   * @param num the number of worker threads.
   */
  void SetNumWorkers(uint32_t num) {
    num_workers_ = num;
  }

 private:
  // Private thread co-class
  class TerrierThread {
   public:
    TerrierThread(int cpu_id, ExecutionThreadPool *pool) :
        cpu_id_(cpu_id), pool_(pool) {
#ifndef __APPLE__
      cpu_set_t mask;
      CPU_ZERO(&mask);
      CPU_SET(cpu_id_, &mask);
      int result = sched_setaffinity(0, sizeof(cpu_set_t), &mask);
      TERRIER_ASSERT(result == 0, "sched_setaffinity should succeed");
      numa_region_ = static_cast<numa_region_t>(numa_node_of_cpu(cpu_id));
      TERRIER_ASSERT(numa_region_ >= 0, "cpu_id must be valid");
#else
      numa_region_ = static_cast<numa_region_t>(0);
#endif
    }
    ~TerrierThread = default;

    void RunNextTask() {
      while (!exit_task_loop_) {
        int16_t index = numa_region_;
        for (int16_t i = 0; i < pool_->num_regions_; i++) {
          index = (index + i) % pool_->num_regions_;
          Task task;
          if (!pool_->task_queue_[index].try_pop(task)) continue;

          task();
          return;
        }

        pool_->busy_workers_--;
        std::unique_lock<std::mutex> l(&pool_->task_lock_)
        pool_->task_cv_.wait(&l);
        pool_->busy_workers_++;

      }
    }

    /*
     * Implements the DedicatedThreadTask api
     */
    void RunTask() {
      pool_->busy_workers_++;
      while (true) {
        if (exit_task_loop_) {
          done_cv_.notify_all();
          return;
        }

        RunNextTask();
      }
    }

    /*
     * Implements the DedicatedThreadTask api
     */
    void Terminate() {
      std::unique_lock<std::mutex> l(&cv_mutex_);
      exit_task_loop_ = true;
      pool->task_cv_.notify_all();
      done_cv_.wait(&l);
      pool_->busy_workers_--;
      pool_->total_workers_--;
    }

    std::mutex cv_mutex_;
    std::condition_variable done_cv_;
    ExecutionThreadPool *pool_;
    int cpu_id_;
    ThreadStatus status = ThreadStatus::SWITCHING;
    numa_region_t numa_region_;
    std::atomic_bool exit_task_loop_ = false;
  };

  common::ManagedPointer<DedicatedThreadRegistry> thread_registry_;
  // Number of NUMA regions
  // The worker threads
  common::SharedLatch array_latch_;
#ifdef __APPLE__
  uint64_t num_regions_ = 1;
#else
  uint64_t num_regions_ = numa_max_node();
#endif
  std::array<std::vector<TerrierThread*>, num_regions_> workers_;
  std::array<TaskQueue, num_regions_> task_queue_;

  std::atomic<uint32_t> busy_workers_, total_workers_;

  std::mutex task_lock_;
  std::condition_variable task_cv_;

  void AddThread(common::ManagedPointer<TerrierThread> thread) {
    common::SharedLatch::ScopedExclusiveLatch l(&array_latch_);
    total_workers_++;
    auto *vector = &workers_[static_cast<int16_t>(thread.operator->().numa_region_)]
    vector->emplace_back(thread.operator->());
  }
  void RemoveThread(common::ManagedPointer<TerrierThread> thread) {
    common::SharedLatch::ScopedExclusiveLatch l(&array_latch_);
    auto *vector = &workers_[static_cast<int16_t>(thread.operator->().numa_region_)]
    vector->remove(vector->begin(), vector->end(), thread.operator->());
  }
  bool OnThreadRemoval(common::ManagedPointer<TerrierThread> thread) {
    // we dont want to deplete a numa region while other numa regions have multiple threads to prevent starvation
    // on the queue for this region
    common::SharedLatch::ScopedSharedLatch l(&array_latch_);
    auto *vector = &workers_[static_cast<int16_t>(thread.operator->().numa_region_)]
    TERRIER_ASSERT(!vector->empty(), "if this thread is potentially being closed it must be tracked");
    // if there is another thread running in this numa region
    if (vector->size() > 1) return true;
    for (int16_t i = 0; i < num_regions_; i++0) {
      vector = &workers_[i];
      TERRIER_ASSERT(!vector->empty(), "if this thread is potentially being closed it must be tracked");
      // if another numa region has extra threads
      if (vector->size() > 1) return false;
    }
    // this numa region is either empty
    return true;
  }
};
}  // namespace terrier::common
