#pragma once

#include <chrono>  // NOLINT

namespace noisepage::execution::util {

/**
 * A simple restartable timer.
 */
template <typename ResolutionRatio = std::milli>
class Timer {
  using Clock = std::chrono::high_resolution_clock;
  using TimePoint = std::chrono::time_point<Clock>;

 public:
  Timer() noexcept { Start(); }

  /**
   * Start the timer.
   */
  void Start() noexcept { start_ = Clock::now(); }

  /**
   * Stop the timer.
   */
  void Stop() noexcept {
    stop_ = Clock::now();

    elapsed_ = std::chrono::duration_cast<std::chrono::duration<double, ResolutionRatio>>(stop_ - start_).count();
  }

  /**
   * @return The total number of elapsed time units.
   */
  double GetElapsed() const noexcept { return elapsed_; }

 private:
  TimePoint start_;
  TimePoint stop_;

  double elapsed_{0};
};

/**
 * Measure the time taken evaluation the functor @em func.
 *
 * @code
 * auto time_ns = Time<std::nano>([] {
 *   // your busy work ...
 * });
 * @endcode
 *
 * @tparam ResolutionRatio Timing resolution, std::milli, std::micro, std::nano etc.
 * @tparam F A no-arg void return functor-type
 * @param f The functor to time
 * @return The elapsed time in whatever resolution ratio the caller wants
 */
template <typename ResolutionRatio, typename F>
inline double Time(F &&f) {
  Timer<ResolutionRatio> timer;
  timer.Start();
  f();
  timer.Stop();
  return timer.GetElapsed();
}

/**
 * Measure the time taken to run the provided function @em f in nanoseconds.
 * @tparam F A no-arg void return functor-type.
 * @param fn The functor to time.
 * @return The elapsed time in nanoseconds.
 */
template <typename F>
inline double TimeNanos(F &&f) {
  return Time<std::nano>(f);
}

/**
 * An RAII timer that begins timing upon construction and stops timing when the object goes out of
 * scope. The total elapsed time is written to the output @em elapsed argument.
 *
 * @code
 * double t = 0.0;
 * {
 *   ScopedTimer<std::milli> timer(&t);
 *   // Work ...
 * }
 * // 't' contains the number of elapsed milliseconds spent in the above block
 * @endcode
 */
template <typename ResolutionRatio = std::milli>
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
    *elapsed_ = timer_.GetElapsed();
  }

 private:
  Timer<ResolutionRatio> timer_;
  double *elapsed_;
};

}  // namespace noisepage::execution::util
