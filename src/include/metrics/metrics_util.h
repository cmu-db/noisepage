#pragma once

#include <chrono>  // NOLINT

#include "execution/util/cpu_info.h"

namespace noisepage::metrics {

/**
 * Struct used to represent the hardware context that we are interested in capturing.
 * We currently capture only the CPU frequency.
 */
struct HardwareContext {
  /** CPU Frequency (MHz) */
  double cpu_mhz_;
};

/**
 * Static utility methods for the metrics component
 */
struct MetricsUtil {
  MetricsUtil() = delete;

  /**
   * Time since the epoch (however this architecture defines that) in microseconds. Really only useful as a
   * monotonically increasing time point
   */
  static uint64_t Now() {
    return std::chrono::duration_cast<std::chrono::microseconds>(
               std::chrono::high_resolution_clock::now().time_since_epoch())
        .count();
  }

  /**
   * @return The hardware context to record
   */
  static HardwareContext GetHardwareContext() { return HardwareContext{execution::CpuInfo::Instance()->GetCpuFreq()}; }
};
}  // namespace noisepage::metrics
