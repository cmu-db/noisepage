#pragma once
#include <memory>
#include <thread>  // NOLINT

#include "common/dedicated_thread_task.h"
#include "common/managed_pointer.h"
#include "common/spin_latch.h"

namespace noisepage::common {

class DedicatedThreadRegistry;

/**
 * @brief DedicatedThreadOwner is the base class for all components that
 * needs to manage long running threads inside the system (e.g. GC, thread pool)
 *
 * The interface exposes necessary behavior to @see DedicatedThreadRegistry, so that the system has a centralized record
 * over all the threads currently running, and retains control over those threads for tuning purposes.
 *
 * Owners themselves are able to request threads by calling RegisterDedicatedThread on the central registry. (For now)
 * These requests are always granted, so owners should only request threads they absolutely need. Similarly, owners are
 * able to remove, or give up, threads by calling StopTask on the central registry.
 *
 * The central registry also has the power to grant/remove threads to/from owners. To do so, the central registry first
 * proposes the granting or removal of a thread by calling OnThreadOffered or OnThreadRemoval respectively. The
 * OnThreadOffered and OnThreadRemoval methods are custom code written by the owner that allow it to accept or decline a
 * thread command made by the registry. An owner should only accept a thread if it has a use for it (example: adding
 * more workers for better performance). Similarly, it should only reject a thread removal if it absolutely needs the
 * thread (example: a component needed at minimum some number of workers).
 *
 * If the owner accepts the granting of a thread, the owner is responsible of calling RegisterDedicatedThread on the
 * registry to claim the thread. RegisterDedicatedThread will call AddThread to let the owner know it has claimed the
 * thread.
 *
 * If the owner accepts the removal of a thread, then the registry will stop the task
 * running on the task, clean it up, and call RemoveThread to let the owner know the thread was removed.
 *
 *
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
    common::SpinLatch::ScopedSpinLatch guard(&thread_count_latch_);
    return thread_count_;
  }

 protected:
  /**
   * @param thread_registry dependency injection for owners to use thread registry, needed to get rid of singleton
   * pattern that used to infest the DedicatedThreadRegistry
   */
  explicit DedicatedThreadOwner(common::ManagedPointer<DedicatedThreadRegistry> thread_registry)
      : thread_registry_(thread_registry) {}

  /**
   * pointer to the ThreadRegistry which is probably owned by DBMain or an injector
   */
  const common::ManagedPointer<DedicatedThreadRegistry> thread_registry_;

 private:
  /**
   * Only the DedicatedThreadRegistry should be allowed to call these methods on an owner
   */
  friend class DedicatedThreadRegistry;

  /**
   * Notifies the owner that a new thread has been given to it
   */
  void AddThread() {
    common::SpinLatch::ScopedSpinLatch guard(&thread_count_latch_);
    thread_count_++;
  }

  /**
   * Notifies the owner that a new thread has removed from them
   */
  void RemoveThread() {
    common::SpinLatch::ScopedSpinLatch guard(&thread_count_latch_);
    thread_count_--;
  }

  /**
   * Custom code to be run by each owner when offered a thread by the registry. The owner has the option to decline the
   * new thread if it does not need it. If the owner accepts, its up to the owner to call RegisterDedicatedThread to
   * register their task
   * @return true if owner accepts thread, false if it declines it
   */
  virtual bool OnThreadOffered() { return false; }

  /**
   * Custom code to be run by each owner when the registry would like to remove its thread running the specific task. It
   * is expected that this function blocks until the thread can be dropped safely and accepts the request, or the owner
   * rejects the request to drop its thread.
   * @param task task running on thread that is to be removed
   * @warning After this method returns, the registry is free to delete the task. It is the owner's responsability to
   * properly clean up the task beforehand.
   * @return true if owner allows registry to remove thread, false otherwise
   */
  virtual bool OnThreadRemoval(common::ManagedPointer<DedicatedThreadTask> task) { return true; }

  // Latch to protect thread count
  common::SpinLatch thread_count_latch_;
  // Number of threads this owner has been granted
  size_t thread_count_ = 0;
};
}  // namespace noisepage::common
