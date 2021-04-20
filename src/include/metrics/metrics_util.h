#pragma once

#include <chrono>  // NOLINT
#include <string>

#include "execution/util/cpu_info.h"
#include "metrics/metrics_defs.h"

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
   * Converts a metrics output string to the enum.
   * Mainly used to convert a settings flag (string) to internal enum.
   * @param metrics output string
   * @return MetricsOutput corresponding to it
   */
  static MetricsOutput FromMetricsOutputString(const std::string &metrics) {
    if (metrics == "CSV") return MetricsOutput::CSV;

    if (metrics == "DB") return MetricsOutput::DB;

    if (metrics == "CSV_AND_DB") return MetricsOutput::CSV_AND_DB;

    NOISEPAGE_ASSERT(false, "Unknown metrics type specified");
    return MetricsOutput::CSV;
  }

  /**
   * @return The hardware context to record
   */
  static HardwareContext GetHardwareContext() { return HardwareContext{execution::CpuInfo::Instance()->GetCpuFreq()}; }
};
}  // namespace noisepage::metrics
