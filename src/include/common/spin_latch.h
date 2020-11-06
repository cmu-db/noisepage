#pragma once

#include <tbb/spin_mutex.h>
// TODO(WAN): Prashanth has two macro-text tokens called TRUE and FALSE and he wants to keep it that way.
//  Meanwhile in TBB land, they include boolean.h for some unfathomable reason which on OSX will define
//  TRUE to be 1 and FALSE to be 0 which causes a series of unfortunate events that end in cryptic errors
//  and wastes lots of time, so as a temporary hack... (The same hack is required in util/execution/tpl.cpp)
#undef TRUE
#undef FALSE
#include "common/macros.h"

namespace noisepage::common {

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

}  // namespace noisepage::common
