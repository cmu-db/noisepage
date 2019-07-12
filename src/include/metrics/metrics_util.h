#pragma once

#include <chrono>  // NOLINT

namespace terrier::metrics {

struct MetricsUtil {
  MetricsUtil() = delete;

  static uint64_t Now() {
    return std::chrono::duration_cast<std::chrono::microseconds>(
               std::chrono::high_resolution_clock::now().time_since_epoch())
        .count();
  }
};
}  // namespace terrier::metrics
