#pragma once

#include <tbb/spin_mutex.h>
#include "common/macros.h"

namespace terrier::common {

/**
 * A cheap and easy spin latch, currently wraps tbb::spin_mutex to optimize for various architectures' PAUSE
 * characteristics. See:
 * https://software.intel.com/en-us/articles/a-common-construct-to-avoid-the-contention-of-threads-architecture-agnostic-spin-wait-loops
 */
class SpinLatch {
 public:
  /**
   * @brief Locks the spin latch.
   *
   * If another thread has already locked the spin latch, a call to lock will
   * block execution until the lock is acquired.
   */
  void Lock() { latch_.lock(); }

  /**
   * @brief Tries to lock the spin latch.
   *
   * @return On successful lock acquisition returns true, otherwise returns false.
   */
  bool TryLock() { return latch_.try_lock(); }

  /**
   * @brief Unlocks the spin latch.
   */
  void Unlock() { latch_.unlock(); }

  /**
   * Scoped spin latch that guaranteees releasing the lock when destructed.
   */
  class ScopedSpinLatch {
   public:
    /**
     * Acquire lock on SpinLatch.
     * @param latch pointer to SpinLatch to acquire
     */
    explicit ScopedSpinLatch(SpinLatch *const latch) : spin_latch_(latch) { spin_latch_->Lock(); }

    /**
     * Release lock (if acquired).
     */
    ~ScopedSpinLatch() { spin_latch_->Unlock(); }
    DISALLOW_COPY_AND_MOVE(ScopedSpinLatch)
   private:
    SpinLatch *spin_latch_;
  };

 private:
  tbb::spin_mutex latch_;
};

}  // namespace terrier::common
