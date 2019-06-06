#pragma once
#include <memory>
#include <mutex>   // NOLINT
#include <thread>  // NOLINT
#include <unordered_map>
#include <unordered_set>
#include "common/dedicated_thread_owner.h"
#include "common/dedicated_thread_task.h"
#include "common/macros.h"
#include "common/managed_pointer.h"

namespace terrier {

/**
 * Singleton class responsible for maintaining and dispensing long running
 * (dedicated) threads to other system components. The class also serves
 * as a control panel for the brain component to be able to collect information
 * on threads in the system and modify how threads are allocated.
 */
class DedicatedThreadRegistry {
 public:
  DedicatedThreadRegistry() = default;

  ~DedicatedThreadRegistry() {
    // Note that if registry is shutting down, it doesn't matter whether
    // owners are notified as this class should have the same life cycle
    // as the entire terrier process.

    TearDown();
  }

  /**
   * TearDown function to clear the thread registry and stop all dedicated threads gracefully.
   * @warning This method does give the owners the opportunity to clean up their task
   */
  void TearDown() {
    std::unique_lock<std::mutex> lock(table_latch_);
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

  // TODO(tianyu): Remove when we remove singletons
  /**
   * @return The singleton instance of the DedicatedThreadRegistry
   */
  static DedicatedThreadRegistry &GetInstance() {
    static DedicatedThreadRegistry registry;
    return registry;
  }

  /**
   *
   * Register a task on a thread. The thread registry will initialize the task and run it on a dedicated thread. The
   * registry owns the task object and is incharge of deleting it. The requester only receives a ManagedPointer to the
   * task
   *
   * @tparam T task type to initialize and run
   * @tparam Targs type of arguments to pass to constructor of task
   * @param requester The owner to assign the new thread to
   * @param args arguments to pass to constructor of task
   * @return ManagedPointer to task object
   */
  template <class T, class... Targs>
  common::ManagedPointer<T> RegisterDedicatedThread(DedicatedThreadOwner *requester, Targs... args) {
    std::unique_lock<std::mutex> lock(table_latch_);

    // Create task
    auto *task = new T(args...);
    thread_owners_table_[requester].insert(task);
    requester->GrantNewThread();
    threads_table_.emplace(task, std::thread([=] { task->RunTask(); }));

    return common::ManagedPointer(task);
  }

  /**
   * Stop a registered task. This function will free the task if it is stopped, thus, it is up to the requester (as well
   * as the task's Terminate method) to do any necessary cleanup to the task before calling StopTask
   * @param requester the owner who registered the task
   * @param task the task that was registered
   * @warning StopTask should not be called multiple times with the same task.
   * @return true if task was stopped, false otherwise
   */
  bool StopTask(DedicatedThreadOwner *requester, common::ManagedPointer<DedicatedThreadTask> task) {
    DedicatedThreadTask *task_ptr;
    std::thread *task_thread;
    {
      std::unique_lock<std::mutex> lock(table_latch_);
      // Exposing the raw pointer like this is okay because we own the underlying raw pointer
      auto search = threads_table_.find(task.operator->());
      TERRIER_ASSERT(search != threads_table_.end(), "Task is not registered");
      task_ptr = search->first;
      task_thread = &search->second;
    }

    // Notify requester of removal
    if (!requester->NotifyThreadRemoved(task)) return false;

    // Terminate task, unlock during termination of thread since we aren't touching the metadata tables
    task->Terminate();
    task_thread->join();

    // Clear Metadata
    {
      std::unique_lock<std::mutex> lock(table_latch_);
      requester->RemoveThread();
      threads_table_.erase(task_ptr);
      thread_owners_table_[requester].erase(task_ptr);
    }
    delete task_ptr;
    return true;
  }

  // TODO(tianyu, gus): Add code for thread removal from thread owner without specifying task. In this case, the task
  // owner can decide which task to sacrifice

 private:
  // Latch to protect internal tables
  std::mutex table_latch_;
  // Using raw pointer is okay since we never dereference said pointer,
  // but only use it as a lookup key
  std::unordered_map<DedicatedThreadTask *, std::thread> threads_table_;
  // Using raw pointer here is also fine since the owner's life cycle is
  // not controlled by the registry
  std::unordered_map<DedicatedThreadOwner *, std::unordered_set<DedicatedThreadTask *>> thread_owners_table_;
};

}  // namespace terrier
