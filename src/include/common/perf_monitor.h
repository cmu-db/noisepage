#pragma once

#include <linux/perf_event.h>
#include <sys/ioctl.h>
#include <sys/syscall.h>
#include <unistd.h>
#include <cstring>
#include <iostream>
#include "common/macros.h"

namespace terrier::common {
/**
 * Wrapper around hw perf events provided by the Linux kernel.
 */
class PerfMonitor {
 public:
  /**
   * Represents the struct read_format with PERF_FORMAT_GROUP enabled, PERF_FORMAT_TOTAL_TIME_ENABLED and
   * PERF_FORMAT_TOTAL_TIME_RUNNING disabled. http://www.man7.org/linux/man-pages/man2/perf_event_open.2.html
   */
  struct PerfCounters {
    uint64_t num_events_;
    uint64_t cpu_cycles_;
    uint64_t instructions_;
    uint64_t cache_references_;
    uint64_t cache_misses_;
    // TODO(Matt): there seems to be a bug with enabling these counters along with the cache counters. When enabled,
    // just get 0s out of all of the counters. Eventually we might want them but can't enable them right now.
    // https://lkml.org/lkml/2018/2/13/810
    // uint64_t branch_instructions_;
    // uint64_t branch_misses_;
    uint64_t bus_cycles_;
    uint64_t ref_cpu_cycles_;

    void Print() const {
      std::cout << "CPU Cycles: " << cpu_cycles_ << std::endl;
      std::cout << "Instructions: " << instructions_ << std::endl;
      std::cout << "Cache References: " << cache_references_ << std::endl;
      std::cout << "Cache Misses: " << cache_misses_ << std::endl;
      // std::cout << "Branch Instructions: " << branch_instructions_ << std::endl;
      // std::cout << "Branch Misses: " << branch_misses_ << std::endl;
      std::cout << "Bus Cycles: " << bus_cycles_ << std::endl;
      std::cout << "Reference CPU Cycles: " << ref_cpu_cycles_ << std::endl << std::endl;
    }

    PerfCounters &operator-=(const PerfCounters &rhs) {
      TERRIER_ASSERT(this->num_events_ == rhs.num_events_,
                     "Operating on PerfCounters objects with different num_events_ doesnt' make sense.");
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

    friend PerfCounters operator-(PerfCounters lhs, const PerfCounters &rhs) {
      lhs -= rhs;
      return lhs;
    }
  };

  PerfMonitor() {
    // Initialize perf configuration
    perf_event_attr pe;
    std::memset(&pe, 0, sizeof(perf_event_attr));
    pe.type = PERF_TYPE_HARDWARE;
    pe.size = sizeof(perf_event_attr);
    pe.disabled = 1;
    pe.exclude_kernel = 1;
    pe.exclude_hv = 1;
    pe.read_format = PERF_FORMAT_GROUP;

    // Open file descriptors for each perf_event that we want. We reuse the first entry of the array as the group fd.
    for (uint8_t i = 0; i < NUM_HW_EVENTS; i++) {
      pe.config = HW_EVENTS[i];
      event_files_[i] = syscall(__NR_perf_event_open, &pe, 0, -1, event_files_[0], 0);
      TERRIER_ASSERT(event_files_[i] > 0, "Failed to open perf_event.");
    }
  }

  ~PerfMonitor() {
    for (const auto i : event_files_) {
      const auto result UNUSED_ATTRIBUTE = close(i);
      TERRIER_ASSERT(result == 0, "Failed to close perf_event.");
    }
  }

  DISALLOW_COPY_AND_MOVE(PerfMonitor)

  void Start() {
    auto result UNUSED_ATTRIBUTE = ioctl(event_files_[0], PERF_EVENT_IOC_RESET, PERF_IOC_FLAG_GROUP);
    TERRIER_ASSERT(result >= 0, "Failed to reset events.");
    result = ioctl(event_files_[0], PERF_EVENT_IOC_ENABLE, PERF_IOC_FLAG_GROUP);
    TERRIER_ASSERT(result >= 0, "Failed to enable events.");
    running_ = true;
  }

  void Stop() {
    TERRIER_ASSERT(running_, "StopEvents() called without StartEvents() first.");
    auto result UNUSED_ATTRIBUTE = ioctl(event_files_[0], PERF_EVENT_IOC_DISABLE, PERF_IOC_FLAG_GROUP);
    TERRIER_ASSERT(result >= 0, "Failed to disable events.");
    running_ = false;
  }

  PerfCounters ReadCounters() {
    PerfCounters counters{};  // zero initialization
    const auto bytes_read UNUSED_ATTRIBUTE = read(event_files_[0], &counters, sizeof(PerfCounters));
    TERRIER_ASSERT(bytes_read == sizeof(PerfCounters), "Failed to read the entire struct.");
    TERRIER_ASSERT(counters.num_events_ == NUM_HW_EVENTS, "Failed to read the correct number of events.");
    return counters;
  }

 private:
  static constexpr uint8_t NUM_HW_EVENTS = 6;
  // set the first file descriptor to -1. Since event_files[0] is always passed into group_fd on
  // perf_event_open, this has the effect of making the first event the group leader. All subsequent syscalls can use
  // that fd.
  std::array<int32_t, NUM_HW_EVENTS> event_files_{-1};
  bool running_ = false;

  static constexpr std::array<uint64_t, NUM_HW_EVENTS> HW_EVENTS{
      PERF_COUNT_HW_CPU_CYCLES,   PERF_COUNT_HW_INSTRUCTIONS, PERF_COUNT_HW_CACHE_REFERENCES,
      PERF_COUNT_HW_CACHE_MISSES, PERF_COUNT_HW_BUS_CYCLES,   PERF_COUNT_HW_REF_CPU_CYCLES};
};
}  // namespace terrier::common
