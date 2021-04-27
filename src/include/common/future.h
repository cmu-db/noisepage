#pragma once

#include <atomic>
#include <condition_variable>  // NOLINT
#include <optional>
#include <string>
#include <thread>  // NOLINT
#include <utility>

namespace noisepage::common {

/**
 * Future is a wrapper over a condition_variable, and the condition.
 * The primary benefits to using this Future over a std::future<>:
 *   - No need to rely on the presence of a std::promise<> object
 *
 *   - Wait() interface allows returning both an indicator of whether
 *     the operation succeeded or not and the result value. This Future
 *     also encapsulates a proper error message.
 *
 *   - Does not depend on an std::exception_ptr to indicate an operational
 *     failure. Does not rely on an encapsulated object that captures
 *     result, success indicator, and error message.
 *
 * If the use-case is merely to coordinate a value (i.e., result of some
 * computation guaranted to complete) between an asynchronous
 * task and a caller (with exceptions being supported for error cases),
 * a regular C++ promise/future setup should be used instead.
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
   * Suspends the current thread and wait up to a limited time for the result to be ready.
   * @param wait_millis The duration that the current thread should wait, in milliseconds.
   * @return The (Result, success) state if the future did NOT time out. Otherwise, nullopt.
   */
  std::optional<std::pair<Result, bool>> WaitFor(std::chrono::milliseconds wait_millis) {
    bool timed_out;
    {
      std::unique_lock<std::mutex> lock(mtx_);

      // Wait until the future is completed by someone with successful result or failure
      timed_out = !cvar_.wait_for(lock, wait_millis, [&] { return done_.load(); });
    }

    return timed_out ? std::nullopt : std::make_optional(std::pair<Result, bool>(result_, success_));
  }

  /**
   * Suspends the current thread and wait for the result to be ready.
   * This wait is dangerous to use, do not use it unless you guarantee that the future will be signaled no matter what.
   * An example that you should handle is simply shutting down the DBMS -- can you guarantee that the future will not
   * be stuck waiting? This would cause the DBMS to be stuck until forcefully killed, which results in the DBMS having
   * a bad exit code, and thus it will fail CI, so on and so forth.
   *
   * @warning This wait is dangerous to use since the DBMS will not exit cleanly if still waiting. You have been warned.
   *
   * @return Result, and success/fail
   */
  std::pair<Result, bool> DangerousWait() {
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
