#pragma once
#include <memory>
#include <thread>  // NOLINT
#include <unordered_map>
#include <vector>
#include "common/dedicated_thread_owner.h"
#include "common/dedicated_thread_task.h"
#include "common/macros.h"

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
   * TearDown function to clear the thread registry and stop all dedicated threads gracefully
   */
  void TearDown() {
    for (auto &entry : thread_owners_table_) {
      for (auto &task : entry.second) {
        task->Terminate();
        threads_table_[task.get()].join();
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
   * Register a thread under requester to run the given task
   *
   * @param requester The owner to assign the new thread to
   * @param task The task to run in the dedicated thread
   * @return the DedicatedThreadTask running on new thread
   */
  template <typename Task>
  void RegisterDedicatedThread(DedicatedThreadOwner *requester, std::shared_ptr<Task> task) {
    thread_owners_table_[requester].push_back(task);
    requester->NotifyNewThread();
    TERRIER_ASSERT(threads_table_.find(task.get()) == threads_table_.end(), "Task is already registered");
    threads_table_.emplace(task.get(), std::thread([=] { task->RunTask(); }));
  }

  // TODO(tianyu): Add code for thread removal

 private:
  // Using raw pointer is okay since we never dereference said pointer,
  // but only use it as a lookup key
  std::unordered_map<DedicatedThreadTask *, std::thread> threads_table_;
  // Using raw pointer here is also fine since the owner's life cycle is
  // not controlled by the registry
  std::unordered_map<DedicatedThreadOwner *, std::vector<std::shared_ptr<DedicatedThreadTask>>> thread_owners_table_;
};

}  // namespace terrier
