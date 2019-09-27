#pragma once

#if __APPLE__
/**
 * Apple doesn't allow per-thread information from the get_rusage() syscall, so instead we resort to Mach calls.
 */
#include <mach/mach_init.h>
#include <mach/mach_port.h>
#include <mach/thread_act.h>
#else
/**
 * Modern Linux systems should allow us to use the get_rusage() syscall for per-thread information.
 */
#include <sys/resource.h>
#include <sys/time.h>
#endif
#include "common/macros.h"

namespace terrier::common {

/**
 * CPU timer for a thread. After calling Start() and Stop(), you can get a CPUTime object with ElapsedTime() to see the
 * user and kernel CPU time. If Start() and Stop() are called on the same object from separate threads then the results
 * will likely be garbage.
 */
class ThreadCPUTimer {
 public:
  /**
   * Struct that represents the user and kernel CPU time of the calling thread
   */
  struct CPUTime {
    /**
     * User CPU time in microseconds
     */
    int64_t user_time_us_;
    /**
     * Kernel CPU time in microseconds
     */
    int64_t system_time_us_;

   private:
    friend class ThreadCPUTimer;
    CPUTime operator-(const CPUTime &rhs) {
      return {this->user_time_us_ - rhs.user_time_us_, this->system_time_us_ - rhs.system_time_us_};
    }
  };

  /**
   * Start the CPU timer for this thread
   */
  void Start() {
    TERRIER_ASSERT(!running_, "Start() called while already running.");
    start_ = Now();
    running_ = true;
  }

  /**
   * Stop the CPU timer for this thread
   */
  void Stop() {
    TERRIER_ASSERT(running_, "Stop() called while not running.");
    end_ = Now();
    running_ = false;
  }

  /**
   * @return ThreadTime object for user and kernel CPU time for this thread
   */
  CPUTime ElapsedTime() {
    TERRIER_ASSERT(!running_, "ElapsedTime() called while still running.");
    return end_ - start_;
  }

 private:
  CPUTime start_;
  CPUTime end_;
  bool running_ = false;

#if __APPLE__
  int64_t TimeValueToMicroseconds(const time_value_t t) const {
    return static_cast<int64_t>(t.seconds) * 1e6 + t.microseconds;
  }
#else
  int64_t TimeValueToMicroseconds(const timeval t) const { return static_cast<int64_t>(t.tv_sec) * 1e6 + t.tv_usec; }
#endif

  /**
   * Platform-specific representation of the current thread's CPU time
   */
  CPUTime Now() const {
#if __APPLE__
    /**
     * Most of this comes from reading:
     * https://www.gnu.org/software/hurd/gnumach-doc/Thread-Information.html
     * http://gnu.ist.utl.pt/software/hurd/gnumach-doc/mach_7.html
     * https://web.mit.edu/darwin/src/modules/xnu/osfmk/man/thread_basic_info.html
     */
    thread_basic_info_data_t basic_info;
    mach_msg_type_number_t count = THREAD_BASIC_INFO_COUNT;
    mach_port_t thread = mach_thread_self();  // open a port right
    kern_return_t const result =
        thread_info(thread, THREAD_BASIC_INFO, reinterpret_cast<thread_info_t>(&basic_info), &count);
    mach_port_deallocate(mach_task_self(), thread);  // close the port right
    if (result == KERN_SUCCESS) {
      return {TimeValueToMicroseconds(basic_info.user_time), TimeValueToMicroseconds(basic_info.system_time)};
    }
    return {-1, -1};
#else
    /**
     * This is just based on:
     * https://linux.die.net/man/2/getrusage
     */
    rusage usage;
    if (getrusage(RUSAGE_THREAD, &usage) == 0) {
      return {TimeValueToMicroseconds(usage.ru_utime), TimeValueToMicroseconds(usage.ru_stime)};
    }
    return {-1, -1};
#endif
  }
};
}  // namespace terrier::common
