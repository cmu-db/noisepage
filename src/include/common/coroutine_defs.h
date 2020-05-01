#pragma once

#include <utility>

#include <boost/coroutine2/all.hpp>

#include "common/object_pool.h"

namespace terrier::common {

using pull_type = boost::coroutines2::coroutine<void>::pull_type;
using push_type = boost::coroutines2::coroutine<void>::push_type;

/**
 * A reusable stack frame context which contains a workload function to be set.
 *
 * This context controls task switching with the execution pool, and may yield out of a running task on a call to
 * YieldToPool() and temporarily stop on a task on a call to YieldToFunc() or complete the workload.
 */
class PoolContext {
 public:
  /**
   * Sets the function associated with the current execution context
   * @param f input function that will serve as workload for the pool context
   */
  void SetFunction(const std::function<void(PoolContext *)> &f) {
    TERRIER_ASSERT(func_ == nullptr, "function should be null");
    this->func_ = f;
  }

  /**
   * Call will pause the workload and yield back to the execution pool.
   */
  void YieldToPool() {
    TERRIER_ASSERT(sink_ != nullptr, "must have initialized sink_ before yielding to it");
    (*sink_)();
  }

  /**
   * @warning ONLY CALL IF YOU KNOW WHAT YOU'RE DOING this executes the function inside the context until it yields back
   *
   * Call will initialize or continue the resumeable workload from the last yield
   * @return func_finished_ will signify whether the current function has finished its execution path
   */
  bool YieldToFunc() {
    TERRIER_ASSERT(func_ != nullptr, "must have called SetFunction before yielding to function");
    TERRIER_ASSERT(sink_ != nullptr, "must have initialized sink_ before yielding to function");
    TERRIER_ASSERT(in_, "in_ should always have yielded");
    in_();
    TERRIER_ASSERT(in_, "in_ should always have yielded");
    return func_finished_;
  }

  ~PoolContext() = default;
  PoolContext() = default;

  /**
   * Internal allocator class that implements object pool interface
   */
  class Allocator {
   public:
    /**
     * @return Allocates a new pool context for use
     */
    PoolContext *New() { return new PoolContext(); }
    /**
     * Defined to implement object pool interface
     */
    void Reuse(PoolContext *const reuse) {}
    /**
     * Deletes an existing pool context
     * @param ptr pointer to existing pool context to be deleted
     */
    void Delete(PoolContext *const ptr) { delete ptr; }
  };

 private:
  // current function that to be executed in the coroutine
  std::function<void(PoolContext *)> func_ = nullptr;
  // pointer to push_type coroutine passed in by boost library this allows the
  push_type *sink_ = nullptr;
  // Initialization of in_ will yield back to execution pool to allow setting of function before running workload
  pull_type in_ = pull_type([&](push_type &s) {  // NOLINT
    this->sink_ = &s;                            // set sink so that function can yield back
    while (true) {
      // yield to pool to allow the pool to assign function and start it
      this->YieldToPool();
      TERRIER_ASSERT(this->func_ != nullptr, "should have initialized function before yielding to function");
      this->func_finished_ = false;  // set function status for return value of YieldToFunc
      this->func_(this);             // run function
      this->func_finished_ = true;   // set function status for return value of YieldToFunc
      this->func_ = nullptr;
      // loop so that the stack allocated in boost library for this coroutine is reused on the next function assigned
    }
  });
  bool func_finished_ = false;
};

using PoolContextPool = ObjectPool<common::PoolContext, common::PoolContext::Allocator>;

}  // namespace terrier::common
