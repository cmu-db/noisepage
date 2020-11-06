#pragma once

#include <chrono>  // NOLINT

#include "common/macros.h"

namespace noisepage::common {

/**
 * The ScopedTimer provided an easy way to collect the elapsed time for a block of code. It stores the current time when
 * it is instantiated, and then when it is destructed it updates the elapsed milliseconds time in the value pointed to
 * by the constructor argument. This is meant for debugging the performance impact of components that don't have
 * dedicate benchmarks, and possibly for use with performance counters in the future. Its output shouldn't be used as
 * pass/fail criteria in any Google Benchmarks (use the framework for that) or Google Tests (performance is variable
 * from machine to machine).
 * @tparam resolution Precision for the timer, i.e. std::chrono::nanoseconds, std::chrono::microseconds,
 * std::chrono::milliseconds, std::chrono::seconds
 */
template <class resolution>
class ScopedTimer {
 public:
  /**
   * Constucts a ScopedTimer. This marks the beginning of the timer's elapsed time.
   * @param elapsed pointer to write the elapsed time (milliseconds) on destruction
   */
  explicit ScopedTimer(uint64_t *const elapsed)
      : start_(std::chrono::high_resolution_clock::now()), elapsed_(elapsed) {}

  /**
   * This marks the end of the timer's elapsed time. The ScopedTimer will update the original constructor argument's
   * value on destruction.
   */
  ~ScopedTimer() {
    auto end = std::chrono::high_resolution_clock::now();
    *elapsed_ = static_cast<uint64_t>(std::chrono::duration_cast<resolution>(end - start_).count());
  }
  DISALLOW_COPY_AND_MOVE(ScopedTimer)
 private:
  const std::chrono::high_resolution_clock::time_point start_;
  uint64_t *const elapsed_;
};
}  // namespace noisepage::common
