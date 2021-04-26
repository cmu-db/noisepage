#include "task/task_runner.h"
#include "execution/compiler/executable_query.h"
#include "planner/plannodes/output_schema.h"
#include "task/task_manager.h"
#include "util/query_exec_util.h"

namespace noisepage::task {

TaskRunner::TaskRunner(std::unique_ptr<util::QueryExecUtil> query_exec_util,
                       common::ManagedPointer<task::TaskManager> manager)
    : query_exec_util_(std::move(query_exec_util)), task_manager_(manager) {}

void TaskRunner::RunTask() {
  // keep the thread alive
  while (true) {
    std::unique_ptr<Task> task = task_manager_->GetTaskWithKillFlag(&kill_);
    if (task) {
      task->Execute(common::ManagedPointer(query_exec_util_), task_manager_);
      task_manager_->FinishTask();
    } else if (kill_) {
      // Woken up by kill flag
      return;
    }
  }
}

void TaskRunner::Terminate() { task_manager_->MarkTerminateFlag(&kill_); }

}  // namespace noisepage::task
