#pragma once

#include "common/dedicated_thread_owner.h"
#include "task/task.h"
#include "task/task_runner.h"

#include <condition_variable>
#include <mutex>
#include <queue>
#include <vector>

namespace noisepage::util {
class QueryExecUtil;
}

namespace noisepage::task {

class TaskRunner;

class TaskManager : public common::DedicatedThreadOwner {
 public:
  TaskManager(common::ManagedPointer<common::DedicatedThreadRegistry> thread_registry,
              std::unique_ptr<util::QueryExecUtil> util, uint32_t num_workers);

  ~TaskManager();

  void Flush();
  void SetTaskPoolSize(int num_workers);
  void AddTask(std::unique_ptr<Task> task);

  /**
   * TODO(wz2): These need to be revisited when the thread registry can scale
   */
  bool OnThreadOffered() { return false; }
  bool OnThreadRemoval(common::ManagedPointer<common::DedicatedThreadTask> task) { return true; }

 private:
  friend class TaskRunner;
  std::unique_ptr<Task> GetTaskWithKillFlag(bool *kill);
  void MarkTerminateFlag(bool *kill);
  void FinishTask();

  std::unique_ptr<util::QueryExecUtil> master_util_;

  uint32_t busy_workers_ = 0;
  std::mutex queue_mutex_;
  std::condition_variable queue_cv_;
  std::condition_variable notify_cv_;
  std::queue<std::unique_ptr<Task>> queue_;

  std::mutex runners_mutex_;
  std::vector<common::ManagedPointer<task::TaskRunner>> task_runners_;
};

}  // namespace noisepage::task
