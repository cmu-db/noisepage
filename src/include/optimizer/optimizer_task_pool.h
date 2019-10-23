#pragma once

#include <memory>
#include <stack>
#include "optimizer/optimizer_task.h"

namespace terrier::optimizer {

/**
 * The base class of a task pool, which needs to support adding tasks and
 * getting available tasks from the pool. Note that a single-threaded task pool
 * is identical to a stack but we may need to implement a different data
 * structure for multi-threaded optimization.
 */
class OptimizerTaskPool {
 public:
  /**
   * Virtual interface function for removing a task from the pool
   */
  virtual OptimizerTask *Pop() = 0;

  /**
   * Virtual interface function for adding a task to the pool
   * @param task OptimizerTask to add
   */
  virtual void Push(OptimizerTask *task) = 0;

  /**
   * Virtual interface function to check whether the pool is empty
   */
  virtual bool Empty() = 0;

  /**
   * Trivial destructor
   */
  virtual ~OptimizerTaskPool() = default;
};

/**
 * Stack implementation of the OptimizerTaskPool
 */
class OptimizerTaskStack : public OptimizerTaskPool {
 public:
  /**
   * Implementation of the Pop interface of OptimizerTaskPool
   * @returns Next OptimizerTask to execute
   */
  OptimizerTask *Pop() override {
    // ownership handed off to caller
    auto task = task_stack_.top();
    task_stack_.pop();
    return task;
  }

  /**
   * Destructor for OptimizerTaskStack
   */
  ~OptimizerTaskStack() override {
    while (!task_stack_.empty()) {
      auto task = task_stack_.top();
      task_stack_.pop();
      delete task;
    }
  }

  /**
   * Implementation of the Push interface of OptimizerTaskPool
   * @param task OptimizerTask to add to the task pool
   */
  void Push(OptimizerTask *task) override {
    // ownership trasnferred to stack
    task_stack_.push(task);
  }

  /**
   * Checks whether the stack is empty or not
   * @returns TRUE if empty
   */
  bool Empty() override { return task_stack_.empty(); }

 private:
  /**
   * Stack for tracking tasks
   */
  std::stack<OptimizerTask *> task_stack_;
};

}  // namespace terrier::optimizer
