#pragma once

#include "optimizer/optimizer_task.h"
#include <stack>
#include <memory>

namespace terrier {
namespace optimizer {
/**
 * @brief The base class of a task pool, which needs to support adding tasks and
 *  getting available tasks from the pool. Note that a single-threaded task pool
 *  is identical to a stack but we may need to implement a different data
 *  structure for multi-threaded optimization
 */
class OptimizerTaskPool {
 public:
  virtual OptimizerTask* Pop() = 0;
  virtual void Push(OptimizerTask *task) = 0;
  virtual bool Empty() = 0;
  virtual ~OptimizerTaskPool() = default;
};

/**
 * @brief Stack implementation of the task pool
 */
class OptimizerTaskStack : public OptimizerTaskPool {
 public:
  virtual OptimizerTask *Pop() {
    // ownership handed off to caller
    auto task = task_stack_.top();
    task_stack_.pop();
    return task;
  }

  virtual ~OptimizerTaskStack() {
    while (!task_stack_.empty()) {
      delete Pop();
    }
  }

  void Push(OptimizerTask *task) override {
    // ownership trasnferred to stack
    task_stack_.push(task);
  }

  bool Empty() override { return task_stack_.empty(); }

 private:
  std::stack<OptimizerTask*> task_stack_;
};

}  // namespace optimizer
}  // namespace terrier
