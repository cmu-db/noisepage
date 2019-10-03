#pragma once

/**
 * Modern Linux systems should allow us to use the get_rusage() syscall for per-thread information.
 */
#include <sys/resource.h>
#include <sys/time.h>
#include "common/macros.h"

namespace terrier::common {

/**
 * getrusage wrapper. After calling Start() and Stop(), you can get a CPUTime object with ElapsedCPUTime() to see the
 * user and kernel CPU time, or get ElapsedDiskBlocks() for file system activity. If Start() and Stop() are called on
 * the same object from separate threads then the results will likely be garbage.
 */
class ThreadUsage {
 public:
  /**
   * Struct that represents the user and kernel CPU time
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
    friend class ThreadUsage;
    CPUTime operator-(const CPUTime &rhs) const {
      return {this->user_time_us_ - rhs.user_time_us_, this->system_time_us_ - rhs.system_time_us_};
    }
  };

  /**
   * Struct that represents the blocks written and read
   */
  struct DiskBlocks {
    /**
     * Number of file system blocks read
     */
    int64_t read_blocks_;
    /**
     * Number of file system blocks written
     */
    int64_t write_blocks_;

   private:
    friend class ThreadUsage;
    DiskBlocks operator-(const DiskBlocks &rhs) const {
      return {this->read_blocks_ - rhs.read_blocks_, this->write_blocks_ - rhs.write_blocks_};
    }
  };

  /**
   * Start the timer for this thread
   */
  void Start() {
    TERRIER_ASSERT(!running_, "Start() called while already running.");
    start_ = Now();
    running_ = true;
  }

  /**
   * Stop the timer for this thread
   */
  void Stop() {
    TERRIER_ASSERT(running_, "Stop() called while not running.");
    end_ = Now();
    running_ = false;
  }

  /**
   * @return CPUTime object for the user and kernel CPU time
   */
  CPUTime ElapsedCPUTime() {
    TERRIER_ASSERT(!running_, "ElapsedCPUTime() called while still running.");
    const CPUTime start{TimeValueToMicroseconds(start_.ru_utime), TimeValueToMicroseconds(start_.ru_stime)};
    const CPUTime end{TimeValueToMicroseconds(end_.ru_utime), TimeValueToMicroseconds(end_.ru_stime)};
    return end - start;
  }

  /**
   * @return DiskBlocks object for the blocks written and read
   */
  DiskBlocks ElapsedDiskBlocks() {
    TERRIER_ASSERT(!running_, "ElapsedDiskBlocks() called while still running.");
    const DiskBlocks start{start_.ru_inblock, start_.ru_oublock};
    const DiskBlocks end{end_.ru_inblock, end_.ru_oublock};
    return end - start;
  }

 private:
  rusage start_;
  rusage end_;
  bool running_ = false;

  int64_t TimeValueToMicroseconds(const timeval t) const { return static_cast<int64_t>(t.tv_sec) * 1e6 + t.tv_usec; }

  /**
   * Platform-specific representation of the current thread's CPU time
   */
  rusage Now() const {
    rusage usage;
#if __APPLE__
    auto ret UNUSED_ATTRIBUTE = getrusage(RUSAGE_SELF, &usage);
#else
    auto ret UNUSED_ATTRIBUTE = getrusage(RUSAGE_THREAD, &usage);
#endif
    TERRIER_ASSERT(ret == 0, "getrusage failed.");
    return usage;
  }
};
}  // namespace terrier::common
