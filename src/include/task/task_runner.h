#pragma once

#include <memory>

#include "common/dedicated_thread_task.h"
#include "task/task.h"

namespace noisepage::util {
class QueryExecUtil;
}

namespace noisepage::task {

class TaskManager;

/**
 * Runner (i.e., thread) of the task manager that handles asynchronously
 * running any jobs submitted to the task manager.
 */
class TaskRunner : public common::DedicatedThreadTask {
 public:
  /**
   * Constructs a new TaskRunner instance
   * @param query_exec_util Dedicated query execution utility for the runner to use
   * @param manager TaskManager that owns this TaskRunner
   */
  TaskRunner(std::unique_ptr<util::QueryExecUtil> query_exec_util, common::ManagedPointer<task::TaskManager> manager);

  /**
   * Runs the task loop.
   */
  void RunTask() override;

  /**
   * Terminate running of the task loop
   */
  void Terminate() override;

 private:
  /** Kill flag for indicating that TaskRunner should be shut down */
  bool kill_ = false;
  std::unique_ptr<util::QueryExecUtil> query_exec_util_;
  common::ManagedPointer<task::TaskManager> task_manager_;
};

}  // namespace noisepage::task
