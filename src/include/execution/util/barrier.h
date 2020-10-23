#pragma once

#include <condition_variable>  // NOLINT
#include <mutex>               // NOLINT

#include "common/macros.h"

namespace noisepage::execution::util {

/**
 * A cyclic barrier is a synchronization construct that allows multiple threads to wait for each
 * other to reach a common barrier point. The barrier is configured with a particular number of
 * threads (N) and, as each thread reaches the barrier, must wait until the remaining N-1 threads
 * arrive. Once the last thread arrives at the barrier point, all waiting threads proceed and the
 * barrier is reset.
 *
 * The barrier is considered "cyclic" because it can be reused. Barriers proceed in "generations".
 * Each time all threads arrive at the barrier, a new generation begins and threads proceed.
 */
class Barrier {
 public:
  /**
   * Create a new barrier with the given count.
   */
  explicit Barrier(const uint32_t count) : generation_(0), count_(count), reset_value_(count) {}

  /**
   * This class cannot be copied or moved.
   */
  DISALLOW_COPY_AND_MOVE(Barrier);

  /**
   * Wait at the barrier point until all remaining threads arrive.
   */
  void Wait() {
    std::unique_lock<std::mutex> lock(mutex_);

    // If we're the last thread to arrive, bump the generation, reset the count
    // and wake up all waiting threads.
    if (--count_ == 0) {
      generation_++;
      count_ = reset_value_;
      cv_.notify_all();
      return;
    }

    // Otherwise, we wait for some thread (i.e., the last thread to arrive) to
    // bump the generation and notify us.
    const uint32_t gen = generation_;
    cv_.wait(lock, [&]() { return gen != generation_; });
  }

  /**
   * @return The current generation the barrier is in.
   */
  uint32_t GetGeneration() const {
    std::unique_lock<std::mutex> lock(mutex_);
    return generation_;
  }

 private:
  // The mutex used to protect all fields
  mutable std::mutex mutex_;
  // The condition variable threads wait on
  std::condition_variable cv_;

  // The current generation
  uint32_t generation_;
  // The current outstanding count
  uint32_t count_;
  // The value to reset the count to when rolling into a new generation
  uint32_t reset_value_;
};

}  // namespace noisepage::execution::util
