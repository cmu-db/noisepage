#pragma once

#include <memory>
#include <thread>  // NOLINT
#include <unordered_map>
#include <unordered_set>

#include "common/dedicated_thread_owner.h"
#include "common/dedicated_thread_task.h"
#include "common/macros.h"
#include "common/managed_pointer.h"
#include "common/spin_latch.h"
#include "metrics/metrics_manager.h"

namespace noisepage::common {

/**
 * @brief Singleton class responsible for maintaining and dispensing long running
 * (dedicated) threads to other system components.
 *
 * The class also serves as a control panel for the self-driving component to be able to collect information on threads
 * in the system and modify how threads are allocated.
 *
 * Additionally, task owners are able to register or stop threads by calling RegisterDedicatedThread or StopTask
 * respectively.
 */
class DedicatedThreadRegistry {
 public:
  /**
   * @param metrics_manager pointer to the metrics manager if metrics are enabled. Necessary for worker threads to
   * register themselves
   */
  explicit DedicatedThreadRegistry(common::ManagedPointer<metrics::MetricsManager> metrics_manager)
      : metrics_manager_(metrics_manager) {}

  ~DedicatedThreadRegistry() {
    // Note that if registry is shutting down, it doesn't matter whether
    // owners are notified as this class should have the same life cycle
    // as the entire noisepage process.

    TearDown();
  }

  /**
   * TearDown function to clear the thread registry and stop all dedicated threads gracefully.
   * @warning This method does give the owners the opportunity to clean up their task
   */
  void TearDown() {
    common::SpinLatch::ScopedSpinLatch guard(&table_latch_);
    for (auto &entry : thread_owners_table_) {
      for (auto *task : entry.second) {
        task->Terminate();
        threads_table_[task].join();
        delete task;
      }
    }
    threads_table_.clear();
    thread_owners_table_.clear();
  }

  /**
   *
   * Register a task on a thread. The thread registry will initialize the task and run it on a dedicated thread. The
   * registry owns the task object and is in charge of deleting it. The requester only receives a ManagedPointer to the
   * task
   *
   * @tparam T task type to initialize and run
   * @tparam Targs type of arguments to pass to constructor of task
   * @param requester The owner to assign the new thread to
   * @param args arguments to pass to constructor of task
   * @return ManagedPointer to task object
   * @warning RegisterDedicatedThread only registers the thread, it does not guarantee that the thread has been started
   * yet
   */
  template <class T, class... Targs>
  common::ManagedPointer<T> RegisterDedicatedThread(DedicatedThreadOwner *requester, Targs... args) {
    common::SpinLatch::ScopedSpinLatch guard(&table_latch_);
    auto *task = new T(args...);  // Create task
    thread_owners_table_[requester].insert(task);
    threads_table_.emplace(task, std::thread([=] {
                             if (metrics_manager_ != DISABLED) metrics_manager_->RegisterThread();
                             task->RunTask();
                           }));
    requester->AddThread();
    return common::ManagedPointer(task);
  }

  /**
   * Stop a registered task. This is a blocking call, and will only return once the task is terminated and the thread
   * returns. This function will free the task if it is stopped, thus, it is up to the requester (as well as the task's
   * Terminate method) to do any necessary cleanup to the task before calling StopTask. StopTask will call the owners
   * OnThreadRemoval method in order to allow the owner to clean up the task.
   * @param requester the owner who registered the task
   * @param task the task that was registered
   * @warning StopTask should not be called multiple times with the same task.
   * @return true if task was stopped, false otherwise
   */
  bool StopTask(DedicatedThreadOwner *requester, common::ManagedPointer<DedicatedThreadTask> task) {
    DedicatedThreadTask *task_ptr;
    std::thread *task_thread;
    {
      common::SpinLatch::ScopedSpinLatch guard(&table_latch_);
      // Exposing the raw pointer like this is okay because we own the underlying raw pointer
      auto search = threads_table_.find(task.operator->());
      NOISEPAGE_ASSERT(search != threads_table_.end(), "Task is not registered");
      task_ptr = search->first;
      task_thread = &search->second;
    }

    // Notify requester of removal
    if (!requester->OnThreadRemoval(task)) return false;

    // Terminate task, unlock during termination of thread since we aren't touching the metadata tables
    task->Terminate();
    task_thread->join();

    // Clear Metadata
    {
      common::SpinLatch::ScopedSpinLatch guard(&table_latch_);
      requester->RemoveThread();
      threads_table_.erase(task_ptr);
      thread_owners_table_[requester].erase(task_ptr);
    }
    delete task_ptr;
    return true;
  }

  // TODO(Gus, Tianyu): When the self driving infrastructure is in, add code to grant owners threads. This code should
  // call OnThreadGranted.

  // TODO(tianyu, gus): When self driving infrastructure is in, add code for thread removal from thread owner without
  // specifying task. In this case, the task owner can decide which task to sacrifice.

 private:
  // Latch to protect internal tables
  common::SpinLatch table_latch_;
  // Using raw pointer is okay since we never dereference said pointer,
  // but only use it as a lookup key
  std::unordered_map<DedicatedThreadTask *, std::thread> threads_table_;
  // Using raw pointer here is also fine since the owner's life cycle is
  // not controlled by the registry
  std::unordered_map<DedicatedThreadOwner *, std::unordered_set<DedicatedThreadTask *>> thread_owners_table_;
  const common::ManagedPointer<metrics::MetricsManager> metrics_manager_;
};

}  // namespace noisepage::common
