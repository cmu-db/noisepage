#pragma once

#include <sys/resource.h>
#include <sys/time.h>
#include "common/macros.h"

namespace terrier::common {
class RusageMonitor {
 public:
  RusageMonitor() = default;

  DISALLOW_COPY_AND_MOVE(RusageMonitor)

  void Start() {
    TERRIER_ASSERT(!running_, "Start() called while already running.");
    Now(&start_);
    running_ = true;
  }

  void Stop() {
    TERRIER_ASSERT(running_, "Stop() called while not running.");
    Now(&end_);
    running_ = false;
  }

  rusage Usage() const {
    TERRIER_ASSERT(!running_, "Usage() called while still running.");
    return SubtractRusage(end_, start_);
  }

  static int64_t TimevalToMicroseconds(const timeval &t) { return static_cast<int64_t>(t.tv_sec) * 1e6 + t.tv_usec; }

 private:
  rusage start_;
  rusage end_;
  bool running_ = false;

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
   * Platform-specific representation of current rusage
   */
  static void Now(rusage *const usage) {
#if __APPLE__
    auto ret UNUSED_ATTRIBUTE = getrusage(RUSAGE_SELF, usage);
#else
    auto ret UNUSED_ATTRIBUTE = getrusage(RUSAGE_THREAD, usage);
#endif
    TERRIER_ASSERT(ret == 0, "getrusage failed.");
  }
};
}  // namespace terrier::common
