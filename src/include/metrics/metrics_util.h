#pragma once

#include <chrono>  // NOLINT
#include <optional>
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
  static std::optional<MetricsOutput> FromMetricsOutputString(const std::string_view &metrics) {
    std::optional<MetricsOutput> type{std::nullopt};
    if (metrics == "CSV") {
      type = MetricsOutput::CSV;
    } else if (metrics == "DB") {
      type = MetricsOutput::DB;
    } else if (metrics == "CSV_AND_DB") {
      type = MetricsOutput::CSV_AND_DB;
    }
    return type;
  }

  /**
   * @return The hardware context to record
   */
  static HardwareContext GetHardwareContext() { return HardwareContext{execution::CpuInfo::Instance()->GetCpuFreq()}; }
};
}  // namespace noisepage::metrics
