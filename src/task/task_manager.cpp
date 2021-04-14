#include "task/task_manager.h"
#include "common/dedicated_thread_registry.h"
#include "execution/compiler/executable_query.h"
#include "planner/plannodes/output_schema.h"
#include "util/query_exec_util.h"

namespace noisepage::task {

TaskManager::TaskManager(common::ManagedPointer<common::DedicatedThreadRegistry> thread_registry,
                         std::unique_ptr<util::QueryExecUtil> util, uint32_t num_workers)
    : DedicatedThreadOwner(thread_registry), master_util_(std::move(util)) {
  for (uint32_t i = 0; i < num_workers; i++) {
    task_runners_.push_back(thread_registry_->RegisterDedicatedThread<TaskRunner>(
        this, util::QueryExecUtil::ConstructThreadLocal(common::ManagedPointer(master_util_)),
        common::ManagedPointer(this)));
  }
}

TaskManager::~TaskManager() {
  // Ensures that all tasks have been taken
  WaitForFlush();

  // Shutdown all the task runners
  for (auto task : task_runners_) {
    thread_registry_->StopTask(this, task.CastManagedPointerTo<common::DedicatedThreadTask>());
  }
}

void TaskManager::AddTask(std::unique_ptr<Task> task) {
  std::unique_lock<std::mutex> lock(queue_mutex_);
  queue_.push(std::move(task));
  queue_cv_.notify_one();
}

void TaskManager::WaitForFlush() {
  std::unique_lock<std::mutex> lock(queue_mutex_);
  if (queue_.empty() && busy_workers_ == 0) {
    // Queue is empty and no workers processing
    return;
  }

  notify_cv_.wait(lock, [&] { return busy_workers_ == 0 && queue_.empty(); });
}

void TaskManager::SetTaskPoolSize(int num_workers) {
  NOISEPAGE_ASSERT(num_workers > 0, "TaskManager requires at least 1 worker");
  std::unique_lock<std::mutex> lock(runners_mutex_);
  auto unum_workers = static_cast<uint32_t>(num_workers);
  if (unum_workers > task_runners_.size()) {
    while (unum_workers != task_runners_.size()) {
      task_runners_.push_back(thread_registry_->RegisterDedicatedThread<TaskRunner>(
          this, util::QueryExecUtil::ConstructThreadLocal(common::ManagedPointer(master_util_)),
          common::ManagedPointer(this)));
    }
  } else if (unum_workers < task_runners_.size()) {
    while (unum_workers != task_runners_.size()) {
      thread_registry_->StopTask(this, task_runners_.back().CastManagedPointerTo<common::DedicatedThreadTask>());
      task_runners_.pop_back();
    }
  }
}

std::unique_ptr<Task> TaskManager::GetTaskWithKillFlag(bool const *kill) {
  std::unique_ptr<Task> task = nullptr;
  {
    std::unique_lock<std::mutex> lock(queue_mutex_);
    queue_cv_.wait(lock, [&] { return (*kill) || !queue_.empty(); });
    if (*kill && queue_.empty()) {
      // we are shutting down but we should empty the queue first
      return nullptr;
    }

    // Grab a new task
    task = std::move(queue_.front());
    queue_.pop();
    busy_workers_++;
  }

  return task;
}

void TaskManager::FinishTask() {
  {
    // hold the lock for updating the counter
    std::lock_guard<std::mutex> lock(queue_mutex_);
    busy_workers_--;
  }

  notify_cv_.notify_one();
}

void TaskManager::MarkTerminateFlag(bool *kill) {
  {
    std::lock_guard<std::mutex> lock(queue_mutex_);
    *kill = true;
  }

  queue_cv_.notify_all();
}

}  // namespace noisepage::task
