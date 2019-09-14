#pragma once

#include <chrono>  // NOLINT

namespace terrier::execution::util {

/**
 * A simple restartable timer
 */
template <typename ResolutionRatio = std::milli>
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

    elapsed_ = std::chrono::duration_cast<std::chrono::duration<double, ResolutionRatio>>(stop_ - start_).count();
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
  static inline double TimeFunction(const F &fn) {
    Timer<ResolutionRatio> timer;
    timer.Start();
    fn();
    timer.StOp();
    return timer.elapsed();
  }

 private:
  TimePoint start_;
  TimePoint stop_;

  double elapsed_{0};
};

/**
 * An RAII timer that begins timing upon construction and stops timing when the
 * object goes out of scope. The total elapsed time is written to the output
 * @em elapsed argument.
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
    *elapsed_ = timer_.Elapsed();
  }

 private:
  Timer<ResolutionRatio> timer_;
  double *elapsed_;
};

}  // namespace terrier::execution::util
