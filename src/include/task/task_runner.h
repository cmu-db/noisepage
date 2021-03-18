#pragma once

#include "common/dedicated_thread_task.h"
#include "task/task.h"

#include <condition_variable>
#include <mutex>
#include <queue>

namespace noisepage::util {
class QueryExecUtil;
}

namespace noisepage::task {

class TaskManager;

class TaskRunner : public common::DedicatedThreadTask {
 public:
  TaskRunner(std::unique_ptr<util::QueryExecUtil> query_exec_util, common::ManagedPointer<task::TaskManager> manager);

  void RunTask();
  void Terminate();

 private:
  bool kill_ = false;
  std::unique_ptr<util::QueryExecUtil> query_exec_util_;
  common::ManagedPointer<task::TaskManager> task_manager_;
};

}  // namespace noisepage::task
