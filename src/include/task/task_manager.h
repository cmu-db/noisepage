#pragma once

#include <condition_variable>  //NOLINT
#include <memory>
#include <mutex>  //NOLINT
#include <queue>
#include <vector>

#include "common/dedicated_thread_owner.h"
#include "task/task.h"
#include "task/task_runner.h"

namespace noisepage::util {
class QueryExecUtil;
}

namespace noisepage::task {

class TaskRunner;

/**
 * Manages a set of runners for executing tasks.
 */
class TaskManager : public common::DedicatedThreadOwner {
 public:
  /**
   * Constructor
   * @param thread_registry ThreadRegistry for use
   * @param util Dedicated query execution utility
   * @param num_workers Initial number of workers available
   */
  TaskManager(common::ManagedPointer<common::DedicatedThreadRegistry> thread_registry,
              std::unique_ptr<util::QueryExecUtil> util, uint32_t num_workers);

  /** Destructor */
  ~TaskManager() override;

  /**
   * Flush waits for all tasks that have been submitted to the queue
   * to be executed. When this function returns, the queue is known
   * to be empty at some point.
   */
  void WaitForFlush();

  /**
   * Adjusts the size of the TaskManager
   * @param num_workers Number of workers available
   */
  void SetTaskPoolSize(int num_workers);

  /**
   * Submit a task to the TaskManager for execution
   * @param task Task to execute
   */
  void AddTask(std::unique_ptr<Task> task);

  /**
   * TODO(wz2): As far as I am currently aware, the thread registry doesn't
   * actually add or remove or re-balance threads on-demand across the system
   * or in response to tuning. The following two functions will need to be
   * revisited when the thread registry is able to rebalance/adjust threads.
   */
  bool OnThreadOffered() override { return false; }
  bool OnThreadRemoval(common::ManagedPointer<common::DedicatedThreadTask> task) override { return true; }

 private:
  friend class TaskRunner;

  /**
   * Gets a task to execute with a [kill] flag indicator.
   * @param kill Flag used to indicate caller is shutting down
   *        The caller is blocked until there is at least some task
   *        within the queue or the kill flag is set and queue_cv_ has
   *        been signaled.
   * @return task if a task should be executed or nullptr if none
   */
  std::unique_ptr<Task> GetTaskWithKillFlag(bool const *kill);

  /**
   * Marks a given kill flag that was previously passed into
   * GetTaskWithKillFlag and try to wake up the thread that
   * might be blocked on the kill flag
   *
   * @param kill Flag to update
   */
  void MarkTerminateFlag(bool *kill);

  /**
   * Called by a TaskRunner to indicate it has finished processing a task
   * retrieved from GetTaskWithKillFlag
   */
  void FinishTask();

  std::unique_ptr<util::QueryExecUtil> master_util_;

  /** Number of workers executing tasks */
  uint32_t busy_workers_ = 0;
  /** Mutex for protecting queue_ */
  std::mutex queue_mutex_;
  /** Condition variable for notifying queue updates or kill flag updates */
  std::condition_variable queue_cv_;
  /** Condition variable for notifying Flush() waiters */
  std::condition_variable notify_cv_;
  /** Queue of tasks to be executed */
  std::queue<std::unique_ptr<Task>> queue_;

  /** Mutex for protecting task_runners_ */
  std::mutex runners_mutex_;
  /** Vector of task runners */
  std::vector<common::ManagedPointer<task::TaskRunner>> task_runners_;
};

}  // namespace noisepage::task
