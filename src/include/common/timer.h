#pragma once

#include <chrono>  // NOLINT
#include "common/macros.h"

namespace terrier::common {

/**
 * An RAII timer that begins timing upon construction and stops timing when the
 * object goes out of scope. The total elapsed time is written to the output
 * @em elapsed argument.
 */

/**
 * A simple restartable timer
 */
template <class resolution>
class Timer {
  using Clock = std::chrono::high_resolution_clock;
  using TimePoint = std::chrono::time_point<Clock>;

 public:
  Timer() noexcept { Start(); }

  /**
   * Start the timer
   */
  void Start() noexcept { start_ = Clock::now(); }

  /**
   * Stop the timer
   */
  void Stop() noexcept {
    stop_ = Clock::now();
    elapsed_ = static_cast<double>(std::chrono::duration_cast<resolution>(stop_ - start_).count());
    // elapsed_ = std::chrono::duration_cast<std::chrono::duration<double, ResolutionRatio>>(stop_ - start_).count();
  }

  /**
   * Return the total number of elapsed time units
   */
  double Elapsed() const noexcept { return elapsed_; }

  /**
   * Time a function @em func
   * @tparam F A no-arg void return functor-type
   * @param fn The functor to time
   * @return The elapsed time in whatever resolution ratio the caller wants
   */
  template <typename F>
  static double TimeFunction(const F &fn) {
    Timer<resolution> timer;
    timer.Start();
    fn();
    timer.Stop();
    return timer.elapsed();
  }

 private:
  TimePoint start_;
  TimePoint stop_;

  double elapsed_{0};
};

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
   * Constructor
   * @param elapsed output variable of the elapsed time
   */
  explicit ScopedTimer(double *elapsed) noexcept : elapsed_(elapsed) {
    *elapsed_ = 0;
    timer_.Start();
  }

  ~ScopedTimer() {
    timer_.Stop();
    *elapsed_ = timer_.Elapsed();
  }

 private:
  Timer<resolution> timer_;
  double *elapsed_;
};

}  // namespace terrier::common
