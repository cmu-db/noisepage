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
  void StartEvents() {
    perf_event_attr pe;

    std::memset(&pe, 0, sizeof(perf_event_attr));
    pe.type = PERF_TYPE_HARDWARE;
    pe.size = sizeof(perf_event_attr);
    pe.disabled = 1;
    pe.exclude_kernel = 1;
//    pe.exclude_hv = 1;
    pe.read_format = PERF_FORMAT_GROUP;

    for (uint8_t i = 0; i < NUM_HW_EVENTS; i++) {
      pe.config = HW_EVENTS[i];
      event_files_[i] = syscall(__NR_perf_event_open, &pe, 0, -1, event_files_[0], 0);
      TERRIER_ASSERT(event_files_[i] != -1, "Failed to open perf_event.");
    }

    auto result UNUSED_ATTRIBUTE = ioctl(event_files_[0], PERF_EVENT_IOC_RESET, PERF_IOC_FLAG_GROUP);
    TERRIER_ASSERT(result >= 0, "Failed to reset events.");
    result = ioctl(event_files_[0], PERF_EVENT_IOC_ENABLE, PERF_IOC_FLAG_GROUP);
    TERRIER_ASSERT(result >= 0, "Failed to enable events.");

    running_ = true;
  }

  void StopEvents() {
    TERRIER_ASSERT(running_, "StopEvents() called without StartEvents() first.");
    auto result UNUSED_ATTRIBUTE = ioctl(event_files_[0], PERF_EVENT_IOC_DISABLE, PERF_IOC_FLAG_GROUP);
    TERRIER_ASSERT(result >= 0, "Failed to disable events.");
    std::memset(&rf_, 0, sizeof(ReadFormat));
    const auto bytes_read UNUSED_ATTRIBUTE = read(event_files_[0], &rf_, sizeof(ReadFormat));
    TERRIER_ASSERT(bytes_read == sizeof(ReadFormat), "Failed to read the entire struct.");
    running_ = false;
    for (const auto i : event_files_) {
      result = close(i);
      TERRIER_ASSERT(result == 0, "File close failed.");
    }
  }

  uint64_t CacheReferences() const { return rf_.event_values_[0].value_; }

  uint64_t CacheMisses() const { return rf_.event_values_[1].value_; }

  uint64_t BranchInstructions() const { return rf_.event_values_[2].value_; }

  uint64_t BranchMisses() const { return rf_.event_values_[3].value_; }

  uint64_t CPUCycles() const { return rf_.event_values_[4].value_; }

 private:
  static constexpr uint8_t NUM_HW_EVENTS = 5;

  struct ReadFormat {
    uint64_t num_events_; /* The number of events */
    struct {
      uint64_t value_; /* The value of the event */
    } event_values_[NUM_HW_EVENTS];
  } rf_;

  std::array<int32_t, NUM_HW_EVENTS> event_files_{-1};
  bool running_ = false;
  static constexpr std::array<uint64_t, NUM_HW_EVENTS> HW_EVENTS{
      PERF_COUNT_HW_CACHE_REFERENCES, PERF_COUNT_HW_CACHE_MISSES, PERF_COUNT_HW_BRANCH_INSTRUCTIONS,
      PERF_COUNT_HW_BRANCH_MISSES, PERF_COUNT_HW_REF_CPU_CYCLES};
};
}  // namespace terrier::common
