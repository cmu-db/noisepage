#pragma once
#include <memory>
#include <thread>  // NOLINT
#include "common/dedicated_thread_task.h"
#include "common/managed_pointer.h"

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
  virtual ~DedicatedThreadOwner() = default;
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
  void NotifyThreadRemoved(common::ManagedPointer<DedicatedThreadTask> task) {
    thread_count_--;
    OnThreadRemoved(task);
  }

 protected:
  /**
   * Custom code to be run when removing a thread by each owner. It is expected
   * that this function blocks until the thread can be dropped safely
   * @param task task that was removed from thread
   * TODO(tianyu) turn into async if need be
   */
  virtual void OnThreadRemoved(common::ManagedPointer<DedicatedThreadTask> task) {}

  // TODO(Gus): Add a OnThreadGranted method for when registry gives owner a new thread, called by NotifyNewThread
  // TODO(Gus, tianyu): Figure out a way where an owner can reject a new thread, example: LogManager only needs 1
  // TODO(Gus, tianyu): Figure out a way where owner can reject the removal of a thread, example: network dispatcher is
  // using a thread the registry wants to take away

 private:
  size_t thread_count_ = 0;
};
}  // namespace terrier
