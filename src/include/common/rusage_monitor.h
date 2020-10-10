#pragma once

#include <sys/resource.h>
#include <sys/time.h>

#include "common/macros.h"

namespace terrier::common {
/**
 * Helped class around get_rusage syscall that allows to profile specific sections of code. If Start() and Stop() are
 * called on the same object from separate threads then the results will likely be garbage.
 */
class RusageMonitor {
 public:
  /**
   * @param entire_process true to accumulate for process, false for just this threadk
   */
  explicit RusageMonitor(const bool entire_process)
      :
#if __APPLE__
        who_(RUSAGE_SELF)
#else
        who_(entire_process ? RUSAGE_SELF : RUSAGE_THREAD)
#endif

  {
  }

  DISALLOW_COPY_AND_MOVE(RusageMonitor)

  /**
   * Start monitoring rusage by capturing current values
   */
  void Start() {
    TERRIER_ASSERT(!running_, "Start() called while already running.");
    Now(&start_);
    running_ = true;
  }

  /**
   * Stop monitoring rusage by capturing current values
   */
  void Stop() {
    TERRIER_ASSERT(running_, "Stop() called while not running.");
    valid_ = running_;
    Now(&end_);
    running_ = false;
  }

  /**
   * Return rusage for the profiled period
   */
  rusage Usage() const {
    TERRIER_ASSERT(!running_, "Usage() called while still running.");
    if (valid_) {
      return SubtractRusage(end_, start_);
    }
    return rusage{};
  }

  /**
   * Helper method to convert timeval to microseconds. Might eventually live elsewhere, but timeval only used here right
   * now
   * @param t timeval to convert
   * @return timeval converted to microseconds
   */
  static int64_t TimevalToMicroseconds(const timeval &t) { return static_cast<int64_t>(t.tv_sec) * 1e6 + t.tv_usec; }

 private:
  rusage start_;
  rusage end_;
  bool running_ = false;
  bool valid_ = false;
  const int who_;

  static timeval SubtractTimeval(const timeval &lhs, const timeval &rhs) {
    return {lhs.tv_sec - rhs.tv_sec, lhs.tv_usec - rhs.tv_usec};
  }

  static rusage SubtractRusage(const rusage &lhs, const rusage &rhs) {
    return {SubtractTimeval(lhs.ru_utime, rhs.ru_utime),
            SubtractTimeval(lhs.ru_stime, rhs.ru_stime),
            {lhs.ru_maxrss - rhs.ru_maxrss},
            {lhs.ru_ixrss - rhs.ru_ixrss},
            {lhs.ru_idrss - rhs.ru_idrss},
            {lhs.ru_isrss - rhs.ru_isrss},
            {lhs.ru_minflt - rhs.ru_minflt},
            {lhs.ru_majflt - rhs.ru_majflt},
            {lhs.ru_nswap - rhs.ru_nswap},
            {lhs.ru_inblock - rhs.ru_inblock},
            {lhs.ru_oublock - rhs.ru_oublock},
            {lhs.ru_msgsnd - rhs.ru_msgsnd},
            {lhs.ru_msgrcv - rhs.ru_msgrcv},
            {lhs.ru_nsignals - rhs.ru_nsignals},
            {lhs.ru_nvcsw - rhs.ru_nvcsw},
            {lhs.ru_nivcsw - rhs.ru_nivcsw}};
  }

  /**
   * Platform-specific representation of current rusage. On macOS it can only get process info. Thread-specific info is
   * non-POSIX and added in later Linux kernels.
   */
  void Now(rusage *const usage) const {
#if __APPLE__
    auto ret UNUSED_ATTRIBUTE = getrusage(who_, usage);
#else
    auto ret UNUSED_ATTRIBUTE = getrusage(who_, usage);
#endif
    TERRIER_ASSERT(ret == 0, "getrusage failed.");
  }
};  // namespace terrier::common
}  // namespace terrier::common
