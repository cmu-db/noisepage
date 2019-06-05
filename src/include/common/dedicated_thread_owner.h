#pragma once
#include <memory>
#include <mutex>   // NOLINT
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
  size_t GetThreadCount() {
    std::unique_lock<std::mutex> lock(thread_count_latch_);
    return thread_count_;
  }

  /**
   * Notifies the owner that a new thread can be given to it. The thread owner has the opportunity to decline the new
   * thread if it does not need it
   * @warning Should only be called by self driving infrastructure.
   * @return true if owner accepts thread, false if it declines it
   */
  bool NotifyNewThread() { return OnThreadGranted(); }

  /**
   * Notifies the owner that a new thread has been given to it
   */
  void GrantNewThread() {
    std::unique_lock<std::mutex> lock(thread_count_latch_);
    thread_count_++;
  }

  /**
   * Notifies the owner that the thread running task will be terminated. The owner has the opportunity to reject the
   * removal of the thread if it needs it
   * @param task the task to be terminated
   * @return true if owner accepts thread removal, false if it rejects
   */
  bool NotifyThreadRemoved(common::ManagedPointer<DedicatedThreadTask> task) { return OnThreadRemoved(task); }

  /**
   * Notifies the owner that a new thread has removed from them
   */
  void RemoveThread() {
    std::unique_lock<std::mutex> lock(thread_count_latch_);
    thread_count_--;
  }

 protected:
  /**
   * Custom code to be run when offered a thread by each owner. The owner has the option to decline the new thread if it
   * does not need it. If the owner accepts, its up to the owner to call RegisterDedicatedThread to register their task
   * @return true if owner accepts thread, false if it declines it
   */
  virtual bool OnThreadGranted() { return false; }

  /**
   * Custom code to be run when removing a thread by each owner. It is expected
   * that this function blocks until the thread can be dropped safely.
   * @param task task that was removed from thread
   * @return
   */
  virtual bool OnThreadRemoved(common::ManagedPointer<DedicatedThreadTask> task) { return false; }

 private:
  std::mutex thread_count_latch_;
  size_t thread_count_ = 0;
};
}  // namespace terrier
