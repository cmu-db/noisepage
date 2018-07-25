#pragma once

#include <wmmintrin.h>
#include <atomic>

#include "common/macros.h"

namespace terrier {

/**
 * Latch states composing of UNLOCKED and LOCKED
 */
enum class LatchState : bool { UNLOCKED, LOCKED };

/**
 * A cheap and easy spin latch.
 */
class SpinLatch {
 public:
  /**
   * Initializes a spin latch and set its state to UNLOCKED
   */
  SpinLatch() : state_(LatchState::UNLOCKED) {}

  /**
   * @brief Locks the spin latch.
   *
   * If another thread has already locked the spin latch, a call to lock will
   * block execution until the lock is acquired.
   */
  void Lock() {
    while (!TryLock()) {
      _mm_pause();  // helps the cpu to detect busy-wait loop
    }
  }

  /**
   * @brief Checks whether the spin latch is locked.
   *
   * @return Returns true if the spin latch is locked, otherwise returns false.
   */
  bool IsLocked() { return state_.load() == LatchState::LOCKED; }

  /**
   * @brief Tries to lock the spin latch.
   *
   * @return On successful lock acquisition returns true, otherwise returns false.
   */
  bool TryLock() {
    // exchange returns the value before locking, thus we need
    // to make sure the lock wasn't already in LOCKED state before
    return state_.exchange(LatchState::LOCKED, std::memory_order_acquire) != LatchState::LOCKED;
  }

  /**
   * @brief Unlocks the spin latch.
   */
  void Unlock() { state_.store(LatchState::UNLOCKED, std::memory_order_release); }

 private:
  /** The exchange method on this atomic is compiled to a lockfree xchgl
   * instruction */
  std::atomic<LatchState> state_;
};

}  // namespace terrier
