#pragma once

#include <linux/perf_event.h>
#include <sys/ioctl.h>
#include <sys/syscall.h>
#include <unistd.h>
#include <cstring>
#include "common/macros.h"

namespace terrier::common {

class PerfMonitor {
 public:
  struct PerfCounters {
    uint64_t num_events_;
    uint64_t cpu_cycles_;
    uint64_t instructions_;
    uint64_t cache_references_;
    uint64_t cache_misses_;
//    uint64_t branch_instructions_;
//    uint64_t branch_misses_;
    uint64_t bus_cycles_;
    uint64_t ref_cpu_cycles_;
  };

  PerfMonitor() {
    perf_event_attr pe;

    std::memset(&pe, 0, sizeof(perf_event_attr));
    pe.type = PERF_TYPE_HARDWARE;
    pe.size = sizeof(perf_event_attr);
    pe.disabled = 1;
    pe.exclude_kernel = 1;
    pe.exclude_hv = 1;
    pe.read_format = PERF_FORMAT_GROUP;

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
  std::array<int32_t, NUM_HW_EVENTS> event_files_{
      -1};  // set the first file descriptor to -1. Since event_files[0] is always passed into group_fd on
            // perf_event_open, this has the effect of making the first event the group leader
  bool running_ = false;

  static constexpr std::array<uint64_t, NUM_HW_EVENTS> HW_EVENTS{
      PERF_COUNT_HW_CPU_CYCLES,   PERF_COUNT_HW_INSTRUCTIONS,        PERF_COUNT_HW_CACHE_REFERENCES,
      PERF_COUNT_HW_CACHE_MISSES,
      PERF_COUNT_HW_BUS_CYCLES,   PERF_COUNT_HW_REF_CPU_CYCLES};
};
}  // namespace terrier::common
