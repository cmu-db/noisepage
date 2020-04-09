#pragma once

#include <sched.h>
#include <storage/storage_defs.h>

#include <atomic>
#include <condition_variable>  // NOLINT
#include <functional>
#include <future>
#include <iostream>
#include <mutex>  // NOLINT
#include <queue>
#include <string>
#include <thread>  // NOLINT
#include <utility>
#include <vector>

#include "common/macros.h"
#include "dedicated_thread_registry.h"
#include "shared_latch.h"
#include "tbb/concurrent_queue.h"

namespace terrier::common {

/**
 * A task queue is a FIFO list of functions that we will execute.
 * This queue by itself is not threadsafe so the WorkerPool class has to protect
 * it on its own with latches.
 */
// TODO(Deepayan): change from void later
using Task = std::pair<std::promise<void>, std::function<void()>>;
using ExecutionTaskQueue = tbb::concurrent_queue<Task>;

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
class ExecutionThreadPool : DedicatedThreadOwner {
 public:
  enum class ThreadStatus { FREE = 0, BUSY = 1, SWITCHING = 2, PARKED = 3 };

  // NOLINTNEXTLINE  lint thinks it has only one arguement
  ExecutionThreadPool(common::ManagedPointer<DedicatedThreadRegistry> thread_registry, std::vector<int> *cpu_ids)
      : DedicatedThreadOwner(thread_registry),
        thread_registry_(thread_registry),
        workers_(num_regions_),
        task_queue_(num_regions_),
        busy_workers_(0) {
    for (int cpu_id : *cpu_ids) {
      thread_registry_.operator->()->RegisterDedicatedThread<TerrierThread>(this, cpu_id, this);
    }
  }

  /**
   * Destructor. Wake up all workers and let them finish before it's destroyed.
   */
  ~ExecutionThreadPool() {
    std::unique_lock<std::mutex> lock(task_lock_);  // grab the lock
    shutting_down_ = true;
    for (std::vector<TerrierThread *> vector : workers_) {
      for (TerrierThread *t : vector) {
        bool result UNUSED_ATTRIBUTE = thread_registry_.operator->()->StopTask(
            this, common::ManagedPointer(static_cast<DedicatedThreadTask *>(t)));
        TERRIER_ASSERT(result, "StopTask should succeed");
      }
    }
  }

  std::future<void> SubmitTask(std::function<void()> task, storage::numa_region_t numa_hint = storage::UNSUPPORTED_NUMA_REGION) {
    if (numa_hint == storage::UNSUPPORTED_NUMA_REGION) {
      numa_hint = static_cast<storage::numa_region_t>(0);
    }
    Task t({std::promise<void>(), task});
    task_queue_[static_cast<int16_t>(numa_hint)].push(std::move(t));
    if (total_workers_ != busy_workers_) {
      task_cv_.notify_one();
    }

    return t.first.get_future();
  }

  /**
   * Get the number of worker threads in this pool
   *
   * @return The number of worker threads
   */
  uint32_t NumWorkers() const { return total_workers_; }

 private:
  // Private thread co-class
  class TerrierThread : public DedicatedThreadTask {
   public:
    TerrierThread(int cpu_id, ExecutionThreadPool *pool) : pool_(pool),  cpu_id_(cpu_id) {
#ifndef __APPLE__
      cpu_set_t mask;
      CPU_ZERO(&mask);
      CPU_SET(cpu_id_, &mask);
      int result = sched_setaffinity(0, sizeof(cpu_set_t), &mask);
      TERRIER_ASSERT(result == 0, "sched_setaffinity should succeed");
      numa_region_ = static_cast<storage::numa_region_t>(numa_node_of_cpu(cpu_id));
      TERRIER_ASSERT(static_cast<int16_t>(numa_region_) >= 0, "cpu_id must be valid");
#else
      //TODO(emmanuee) figure out processor_assign and put here
      numa_region_ = static_cast<storage::numa_region_t>(0);
#endif
      pool_->total_workers_++;
    }
    ~TerrierThread() = default;

    void RunNextTask() {
      while (UNLIKELY(!exit_task_loop_)) {
        int16_t index = static_cast<int16_t>(numa_region_);
        for (int16_t i = 0; i < pool_->num_regions_; i++) {
          index = (index + 1) % pool_->num_regions_;
          Task task;
          if (!pool_->task_queue_[index].try_pop(task)) continue;

          status_ = ThreadStatus::BUSY;
          task.second();
          task.first.set_value();
          status_ = ThreadStatus::SWITCHING;
          return;
        }

        pool_->busy_workers_--;
        status_ = ThreadStatus::PARKED;
        std::unique_lock<std::mutex> l(pool_->task_lock_);
        pool_->task_cv_.wait(l);
        status_ = ThreadStatus::SWITCHING;
        pool_->busy_workers_++;
      }
    }

    /*
     * Implements the DedicatedThreadTask api
     */
    void RunTask() {
      status_ = ThreadStatus::SWITCHING;
      pool_->busy_workers_++;

      while (LIKELY(!exit_task_loop_)) {
        RunNextTask();
      }

      done_exiting_ = true;
      done_cv_.notify_all();
    }

    /*
     * Implements the DedicatedThreadTask api
     */
    void Terminate() {
      std::unique_lock<std::mutex> l(cv_mutex_);
      exit_task_loop_ = true;
      pool_->task_cv_.notify_all();
      while (!done_exiting_) {
        done_cv_.wait_for(l, std::chrono::milliseconds(10));
      }
      pool_->busy_workers_--;
      pool_->total_workers_--;
    }

    std::mutex cv_mutex_;
    std::condition_variable done_cv_;
    ExecutionThreadPool *pool_;
    int cpu_id_;
    ThreadStatus status_ = ThreadStatus::FREE;
    storage::numa_region_t numa_region_;
    std::atomic_bool exit_task_loop_ = false, done_exiting_ = false;
  };

  common::ManagedPointer<DedicatedThreadRegistry> thread_registry_;
  // Number of NUMA regions
  // The worker threads
  common::SharedLatch array_latch_;
#ifdef __APPLE__
  int16_t num_regions_ = 1;
#else
  int16_t num_regions_ = static_cast<int16_t>(numa_max_node());
#endif
  std::vector<std::vector<TerrierThread *>> workers_;
  std::vector<ExecutionTaskQueue> task_queue_;

  std::atomic<uint32_t> busy_workers_, total_workers_;
  std::atomic_bool shutting_down_ = false;

  std::mutex task_lock_;
  std::condition_variable task_cv_;

  void AddThread(TerrierThread *thread) {
    common::SharedLatch::ScopedExclusiveLatch l(&array_latch_);
    TERRIER_ASSERT(0 <= static_cast<int16_t>(thread->numa_region_) && static_cast<int16_t>(thread->numa_region_) <= num_regions_,"numa region should be in range");
    auto *vector = &workers_[static_cast<int16_t>(thread->numa_region_)];
    vector->emplace_back(thread);
  }
  void RemoveThread(TerrierThread *thread) {
    common::SharedLatch::ScopedExclusiveLatch l(&array_latch_);
    TERRIER_ASSERT(0 <= static_cast<int16_t>(thread->numa_region_) && static_cast<int16_t>(thread->numa_region_) <= num_regions_,"numa region should be in range");
    std::vector<TerrierThread *> *vector = &workers_[static_cast<int16_t>(thread->numa_region_)];
    auto it = vector->begin();
    for (; it != vector->end() && *it != thread; ++it) {
    }
    if (it != vector->end()) {
      vector->erase(it);
    }
  }

  bool OnThreadRemoval(common::ManagedPointer<DedicatedThreadTask> dedicated_task) {
    // we dont want to deplete a numa region while other numa regions have multiple threads to prevent starvation
    // on the queue for this region
    auto *thread = static_cast<TerrierThread *>(dedicated_task.operator->());
    if (shutting_down_) return true;

    common::SharedLatch::ScopedSharedLatch l(&array_latch_);
    TERRIER_ASSERT(0 <= static_cast<int16_t>(thread->numa_region_) && static_cast<int16_t>(thread->numa_region_) <= num_regions_,"numa region should be in range");
    auto *vector = &workers_[static_cast<int16_t>(thread->numa_region_)];
    TERRIER_ASSERT(!vector->empty(), "if this thread is potentially being closed it must be tracked");
    // if there is another thread running in this numa region
    if (vector->size() > 1) return true;
    for (int16_t i = 0; i < num_regions_; i++) {
      vector = &workers_[i];
      // if another numa region has extra threads
      if (vector->size() > 1) return false;
    }
    // no numa region has multiple threads
    return true;
  }
};
}  // namespace terrier::common
