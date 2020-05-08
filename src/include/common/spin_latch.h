#pragma once

#include <tbb/spin_mutex.h>
// TODO(WAN): Prashanth has two macro-text tokens called TRUE and FALSE and he wants to keep it that way.
//  Meanwhile in TBB land, they include boolean.h for some unfathomable reason which on OSX will define
//  TRUE to be 1 and FALSE to be 0 which causes a series of unfortunate events that end in cryptic errors
//  and wastes lots of time, so as a temporary hack...
#undef TRUE
#undef FALSE
#include "common/execution_thread_pool.h"
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
   * @param ctx optional thread pool context to allow context switching
   * @brief Locks the spin latch.
   *
   * If another thread has already locked the spin latch, a call to lock will
   * block execution until the lock is acquired.
   */
  void Lock(common::PoolContext *ctx = nullptr) {
    if (ctx != nullptr) {
      // if we have access to a pool context we try to get the latch. if we fail then we yield control back to the
      // thread pool. We repeat this until we get the latch.
      while (!TryLock()) ctx->YieldToPool();
      return;
    }
    // if no context then just try to get the latch the old fashioned way
    latch_.lock();
  }

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
     * @param ctx optional thread pool context to allow context switching
     */
    explicit ScopedSpinLatch(SpinLatch *const latch, common::PoolContext *ctx = nullptr) : spin_latch_(latch) {
      spin_latch_->Lock(ctx);
    }

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
