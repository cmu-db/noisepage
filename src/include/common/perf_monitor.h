#pragma once

#if __APPLE__
// nothing to include since it doesn't support perf events
#else
#include <linux/perf_event.h>
#include <sys/ioctl.h>
#include <sys/syscall.h>
#endif
#include <unistd.h>

#include <array>
#include <cstring>

#include "common/macros.h"

namespace noisepage::common {
/**
 * Wrapper around hw perf events provided by the Linux kernel. Instantiating and destroying PerfMonitors are a bit
 * expensive because they open multiple file descriptors (read: syscalls). Ideally you want to keep a PerfMonitor object
 * around for a portion of code you want to profile, and then just rely on Start() and Stop().
 * @tparam inherit true means that any threads spawned from this thread after the perf counter instantiation will be
 * accumulated into the parents' counters. This has performance implications. false otherwise (only count this thread's
 * counters, regardless of spawned threads)
 */
template <bool inherit>
class PerfMonitor {
 public:
  /**
   * Represents the struct read_format with PERF_FORMAT_GROUP enabled, PERF_FORMAT_TOTAL_TIME_ENABLED and
   * PERF_FORMAT_TOTAL_TIME_RUNNING disabled. http://www.man7.org/linux/man-pages/man2/perf_event_open.2.html
   */
  struct PerfCounters {
    /**
     * Should always be NUM_HW_EVENTS after a read since that's how many counters we have.
     */
    uint64_t num_counters_;

    /**
     * Total cycles. Be wary of what happens during CPU frequency scaling.
     */
    uint64_t cpu_cycles_;
    /**
     * Retired instructions. Be careful, these can be affected by various issues, most notably hardware interrupt
     * counts.
     */
    uint64_t instructions_;
    /**
     * Cache accesses. Usually this indicates Last Level Cache accesses but this may vary depending on your CPU.  This
     * may include prefetches and coherency messages; again this depends on the design of your CPU.
     */
    uint64_t cache_references_;
    /**
     * Cache misses.  Usually this indicates Last Level Cache misses.
     */
    uint64_t cache_misses_;
    // TODO(Matt): there seems to be a bug with enabling these counters along with the cache counters. When enabled,
    // just get 0s out of all of the counters. Eventually we might want them but can't enable them right now.
    // https://lkml.org/lkml/2018/2/13/810
    // uint64_t branch_instructions_;
    // uint64_t branch_misses_;
    /**
     * Bus cycles, which can be different from total cycles.
     */
    uint64_t bus_cycles_;
    /**
     * Total cycles; not affected by CPU frequency scaling.
     */
    uint64_t ref_cpu_cycles_;

    /**
     * compound assignment
     * @param rhs you know subtraction? this is the right side of that binary operator
     * @return reference to this
     */
    PerfCounters &operator-=(const PerfCounters &rhs) {
      this->cpu_cycles_ -= rhs.cpu_cycles_;
      this->instructions_ -= rhs.instructions_;
      this->cache_references_ -= rhs.cache_references_;
      this->cache_misses_ -= rhs.cache_misses_;
      // this->branch_instructions_ -= rhs.branch_instructions_;
      // this->branch_misses_ -= rhs.branch_misses_;
      this->bus_cycles_ -= rhs.bus_cycles_;
      this->ref_cpu_cycles_ -= rhs.ref_cpu_cycles_;
      return *this;
    }

    /**
     * subtract implemented from compound assignment. passing lhs by value helps optimize chained a+b+c
     * @param lhs you know subtraction? this is the left side of that binary operator
     * @param rhs you know subtraction? this is the right side of that binary operator
     * @return
     */
    friend PerfCounters operator-(PerfCounters lhs, const PerfCounters &rhs) {
      lhs -= rhs;
      return lhs;
    }
  };

  /**
   * Create a perf monitor and open all of the necessary file descriptors.
   */
  PerfMonitor() {
#if __APPLE__
    // Apple doesn't support perf events and currently doesn't expose an equivalent kernel API
    valid_ = false;
#else
    // Initialize perf configuration
    perf_event_attr pe;
    std::memset(&pe, 0, sizeof(perf_event_attr));
    pe.type = PERF_TYPE_HARDWARE;
    pe.size = sizeof(perf_event_attr);
    pe.disabled = 1;
    pe.exclude_kernel = 1;
    pe.exclude_hv = 1;
    if constexpr (inherit) {  // NOLINT
      // Count your children's counters
      pe.inherit = 1;
      pe.inherit_stat = 1;
    } else {  // NOLINT
      // Don't read children thread counters, can optimize to read this thread's counters in group fashion
      pe.read_format = PERF_FORMAT_GROUP;
    }

    // Open file descriptors for each perf_event that we want.
    for (uint8_t i = 0; i < NUM_HW_EVENTS; i++) {
      pe.config = HW_EVENTS[i];
      if constexpr (inherit) {  // NOLINT
        // Each counter is its own group (-1 group fd)
        event_files_[i] = syscall(__NR_perf_event_open, &pe, 0, -1, -1, 0);
      } else {  // NOLINT
        //  We reuse the first entry of the array as the group fd.
        event_files_[i] = syscall(__NR_perf_event_open, &pe, 0, -1, event_files_[0], 0);
      }
      valid_ = valid_ && event_files_[i] > 2;  // 0, 1, 2 are reserved for stdin, stdout, stderr respectively
    }
#endif
  }

  ~PerfMonitor() {
    if (valid_) {
      // Iterate through all of the events' file descriptors and close them
      for (const auto i : event_files_) {
        const auto result UNUSED_ATTRIBUTE = close(i);
        NOISEPAGE_ASSERT(result == 0, "Failed to close perf_event.");
      }
    }
  }

  DISALLOW_COPY_AND_MOVE(PerfMonitor)

  /**
   * Start monitoring perf counters
   */
  void Start() {
#if __APPLE__
    // do nothing
#else
    if (valid_) {
      if constexpr (inherit) {  // NOLINT
        // Iterate through all of the events' file descriptors resetting and starting them
        for (const auto i : event_files_) {
          auto result UNUSED_ATTRIBUTE = ioctl(i, PERF_EVENT_IOC_RESET);
          NOISEPAGE_ASSERT(result >= 0, "Failed to reset events.");
          result = ioctl(i, PERF_EVENT_IOC_ENABLE);
          NOISEPAGE_ASSERT(result >= 0, "Failed to enable events.");
        }
      } else {  // NOLINT
        // Reset all of the counters out with a single syscall.
        auto result UNUSED_ATTRIBUTE = ioctl(event_files_[0], PERF_EVENT_IOC_RESET, PERF_IOC_FLAG_GROUP);
        NOISEPAGE_ASSERT(result >= 0, "Failed to reset events.");
        // Start all of the counters out with a single syscall.
        result = ioctl(event_files_[0], PERF_EVENT_IOC_ENABLE, PERF_IOC_FLAG_GROUP);
        NOISEPAGE_ASSERT(result >= 0, "Failed to enable events.");
      }
      running_ = true;
    }
#endif
  }

  /**
   * Stop monitoring perf counters
   */
  void Stop() {
#if __APPLE__
    // do nothing
#else
    if (valid_) {
      NOISEPAGE_ASSERT(running_, "StopEvents() called without StartEvents() first.");
      if constexpr (inherit) {  // NOLINT
        // Iterate through all of the events' file descriptors stopping them
        for (const auto i : event_files_) {
          auto result UNUSED_ATTRIBUTE = ioctl(i, PERF_EVENT_IOC_DISABLE);
          NOISEPAGE_ASSERT(result >= 0, "Failed to disable events.");
        }
      } else {  // NOLINT
        // Stop all of the counters out with a single syscall.
        auto result UNUSED_ATTRIBUTE = ioctl(event_files_[0], PERF_EVENT_IOC_DISABLE, PERF_IOC_FLAG_GROUP);
        NOISEPAGE_ASSERT(result >= 0, "Failed to disable events.");
      }
      running_ = false;
    }
#endif
  }

  /**
   * Read out counters for the profiled period
   * @return struct representing the counters
   */
  PerfCounters Counters() const {
    PerfCounters counters{};  // zero initialization
    if (valid_) {
      if constexpr (inherit) {  // NOLINT
        // Iterate through all of the events' file descriptors reading them

        auto bytes_read UNUSED_ATTRIBUTE = read(event_files_[0], &counters.cpu_cycles_, sizeof(uint64_t));
        NOISEPAGE_ASSERT(bytes_read == sizeof(uint64_t), "Failed to read the counter.");

        bytes_read = read(event_files_[1], &counters.instructions_, sizeof(uint64_t));
        NOISEPAGE_ASSERT(bytes_read == sizeof(uint64_t), "Failed to read the counter.");

        bytes_read = read(event_files_[2], &counters.cache_references_, sizeof(uint64_t));
        NOISEPAGE_ASSERT(bytes_read == sizeof(uint64_t), "Failed to read the counter.");

        bytes_read = read(event_files_[3], &counters.cache_misses_, sizeof(uint64_t));
        NOISEPAGE_ASSERT(bytes_read == sizeof(uint64_t), "Failed to read the counter.");

        bytes_read = read(event_files_[4], &counters.bus_cycles_, sizeof(uint64_t));
        NOISEPAGE_ASSERT(bytes_read == sizeof(uint64_t), "Failed to read the counter.");

        bytes_read = read(event_files_[5], &counters.ref_cpu_cycles_, sizeof(uint64_t));
        NOISEPAGE_ASSERT(bytes_read == sizeof(uint64_t), "Failed to read the counter.");
      } else {  // NOLINT
        // Read all of the counters out with a single syscall.
        auto bytes_read UNUSED_ATTRIBUTE = read(event_files_[0], &counters, sizeof(PerfCounters));
        NOISEPAGE_ASSERT(bytes_read == sizeof(PerfCounters), "Failed to read the counters.");
        NOISEPAGE_ASSERT(counters.num_counters_ == NUM_HW_EVENTS, "Failed to read the counters.");
      }
    }
    return counters;
  }

  /**
   * Number of currently enabled HW perf events. Update this if more are added.
   */
  static constexpr uint8_t NUM_HW_EVENTS = 6;

 private:
  // set the first file descriptor to -1. Since event_files[0] is always passed into group_fd on
  // perf_event_open, this has the effect of making the first event the group leader. All subsequent syscalls can use
  // that fd if we are not inheriting child counters.
  std::array<int32_t, NUM_HW_EVENTS> event_files_{-1};
  bool valid_ = true;

#if __APPLE__
  // do nothing
#else

  bool running_ = false;
  static constexpr std::array<uint64_t, NUM_HW_EVENTS> HW_EVENTS{
      PERF_COUNT_HW_CPU_CYCLES,   PERF_COUNT_HW_INSTRUCTIONS, PERF_COUNT_HW_CACHE_REFERENCES,
      PERF_COUNT_HW_CACHE_MISSES, PERF_COUNT_HW_BUS_CYCLES,   PERF_COUNT_HW_REF_CPU_CYCLES};
#endif
};
}  // namespace noisepage::common
