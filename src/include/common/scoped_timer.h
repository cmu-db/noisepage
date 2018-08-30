#pragma once

#include <chrono>  // NOLINT
#include "common/macros.h"

namespace terrier::common {

// TODO(Matt): we could templatize this for different resolutions (milliseconds, nanoseconds, etc.) but then the caller
// needs to include the <chrono> library to pass in the correct template arguments, which this is trying to be a wrapper
// for. However, I'm not opposed to the idea of templatizing it for this purpose if we decide milliseconds is not always
// suitable.

/**
 * The ScopedTimer provided an easy way to collect the elapsed time for a block of code. It stores the current time when
 * it is instantiated, and then when it is destructed it updates the elapsed milliseconds time in the value pointed to
 * by the constructor argument.
 */
class ScopedTimer {
 public:
  ScopedTimer() = delete;
  /**
   * Constucts a ScopedTimer. This marks the beginning of the timer's elapsed time.
   * @param elapsed_ms pointer to write the elapsed time (milliseconds) on destruction
   */
  explicit ScopedTimer(uint64_t *const elapsed_ms)
      : start_(std::chrono::high_resolution_clock::now()), elapsed_ms_(elapsed_ms) {}

  /**
   * This marks the end of the timer's elapsed time. The ScopedTimer will update the original constructor argument's
   * value on destruction.
   */
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
