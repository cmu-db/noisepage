#pragma once

#include "tbb/spin_mutex.h"

#include "execution/util/macros.h"

namespace terrier::execution::util {

/**
 * A cheap and easy spin latch, currently wraps tbb::spin_mutex to optimize for
 * various architectures' PAUSE characteristics. See:
 * https://software.intel.com/en-us/articles/a-common-construct-to-avoid-the-contention-of-threads-architecture-agnostic-spin-wait-loops
 */
class SpinLatch {
 public:
  /**
   * Locks the spin latch.
   *
   * If another thread has already locked the spin latch, a call to lock will
   * block execution until the lock is acquired.
   */
  void Lock() { latch_.lock(); }

  /**
   * Tries to lock the spin latch.
   * @return Returns true on acquisition; false otherwise
   */
  bool TryLock() { return latch_.try_lock(); }

  /**
   * Unlocks the spin latch.
   */
  void Unlock() { latch_.unlock(); }

  /**
   * Scoped spin latch that guarantees releasing the lock when destructed.
   */
  class ScopedSpinLatch {
   public:
    /**
     * Acquire lock on SpinLatch.
     * @param latch pointer to SpinLatch to acquire
     */
    explicit ScopedSpinLatch(SpinLatch *latch) : spin_latch_(latch) { spin_latch_->Lock(); }

    /**
     * This class cannot be copied or moved.
     */
    DISALLOW_COPY_AND_MOVE(ScopedSpinLatch)

    /**
     * Release lock
     */
    ~ScopedSpinLatch() { spin_latch_->Unlock(); }

   private:
    SpinLatch *spin_latch_;
  };

 private:
  tbb::spin_mutex latch_;
};

}  // namespace terrier::execution::util
