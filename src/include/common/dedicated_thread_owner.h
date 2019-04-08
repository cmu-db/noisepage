#pragma once
#include <memory>
#include <thread>  // NOLINT
#include "common/dedicated_thread_task.h"

namespace terrier {
/**
 * @brief DedicatedThreadOwner is the base class for all components that
 * needs to manage long running threads inside the system (e.g. GC, thread pool)
 *
 * The interface exposes necessary behavior to @see DedicatedThreadRegistry, so
 * that the system has a centralized record over all the threads currently
 * running, and retains control over those threads for tuning purposes.
 *
 * TODO(tianyu): also add some statistics of thread utilization for tuning
 */
class DedicatedThreadOwner {
 public:
  /**
   * @return the number of threads owned by this owner
   */
  size_t GetThreadCount() { return thread_count_; }

  /**
   * Notifies the owner that a new thread has been given to it
   */
  void NotifyNewThread() { thread_count_++; }

  /**
   * Notifies the owner that the thread running task will be terminated
   * @param task the task to be terminated
   */
  void NotifyThreadRemoved(const std::shared_ptr<DedicatedThreadTask> &task) {
    thread_count_--;
    OnThreadRemoved(task);
  }

 protected:
  /**
   * Custom code to be run when removing a thread by each owner. It is expected
   * that this function blocks until the thread can be dropped safely
   *
   * TODO(tianyu) turn into async if need be
   */
  virtual void OnThreadRemoved(const std::shared_ptr<DedicatedThreadTask> &task) {}

 private:
  size_t thread_count_ = 0;
};
}  // namespace terrier
