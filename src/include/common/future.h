#pragma once

#include <atomic>
#include <condition_variable>  // NOLINT
#include <string>
#include <thread>  // NOLINT
#include <utility>

namespace noisepage::common {

/**
 * Future is a wrapper over a condition_variable, and the condition.
 *
 * It is used to build synchronous API over asynchronous function calls:
 * ```c++
 *      Future future;
 *      AsyncCall(future);
 *
 *      auto result = future.Wait();
 *      bool success = result.second;
 *      auto data = result.first;
 * ```
 *
 * @tparam Result the type of the future's result
 */
template <class Result>
class Future {
 public:
  /**
   * Initialize a future object
   */
  Future() = default;

  /**
   * Suspends the current thread and wait for the result to be ready
   * @return Result, and success/fail
   */
  std::pair<Result, bool> Wait() {
    {
      std::unique_lock<std::mutex> lock(mtx_);

      // Wait until the future is completed by someone with successful result or failure
      cvar_.wait(lock, [&] { return done_.load(); });
    }

    return {result_, success_};
  }

  /**
   * Indicate this future is done with result
   * @param result ModelServer's response
   */
  void Success(const Result &result) {
    {
      std::unique_lock<std::mutex> lock(mtx_);
      result_ = result;
      done_ = true;
      success_ = true;
    }

    // A future will only be completed by one thread, but there could be waited by multiple threads.
    // An example could be multiple threads waiting for the training process completion.
    cvar_.notify_all();
  }

  /**
   * Indicate this future failed to complete its task with a reason.
   */
  void Fail(const std::string &reason) {
    {
      std::unique_lock<std::mutex> lock(mtx_);
      done_ = true;
      success_ = false;
      fail_msg_ = reason;
    }
    cvar_.notify_all();
  }

  /**
   * @return A message describing why the operation failed
   */
  const std::string &FailMessage() const { return fail_msg_; }

 protected:
  /** Result for the future */
  Result result_;

  /** Condition variable for waiter of this future to wait for it being ready */
  std::condition_variable cvar_;

  /** True If async operation done */
  std::atomic<bool> done_ = false;

  /** True If async operation succeeds */
  std::atomic<bool> success_ = false;

  /** Reason for failure */
  std::string fail_msg_;

  /** Mutex associated with the condition variable */
  std::mutex mtx_;
};

}  // namespace noisepage::common
