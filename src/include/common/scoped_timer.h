#pragma once

#include <chrono>  // NOLINT
#include "common/macros.h"

namespace terrier::common {

class ScopedTimer {
 public:
  ScopedTimer() = delete;
  explicit ScopedTimer(uint64_t *const elapsed_ms)
      : start_(std::chrono::high_resolution_clock::now()), elapsed_ms_(elapsed_ms) {}

  ~ScopedTimer() {
    auto end = std::chrono::high_resolution_clock::now();
    *elapsed_ms_ = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(end - start_).count());
  }
  DISALLOW_COPY_AND_MOVE(ScopedTimer)
 private:
  const std::chrono::high_resolution_clock::time_point start_;
  uint64_t *const elapsed_ms_;
};
}  // namespace terrier::common
