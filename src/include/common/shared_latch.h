#pragma once

#include <shared_mutex>

#include "common/macros.h"

namespace noisepage::common {

/**
 * A cheap(?) and easy shared (reader-writer) latch, currently wraps std::shared_mutex.
 */
class SharedLatch {
 public:
  /**
   * Acquire exclusive lock on mutex.
   */
  void LockExclusive() { latch_.lock(); }

  /**
   * Acquire shared lock on mutex.
   */
  void LockShared() { latch_.lock_shared(); }

  /**
   * Try to acquire exclusive lock on mutex.
   * @return true if lock acquired, false otherwise.
   */
  bool TryExclusiveLock() { return latch_.try_lock(); }

  /**
   * Try to acquire shared lock on mutex.
   * @return true if lock acquired, false otherwise.
   */
  bool TryLockShared() { return latch_.try_lock_shared(); }

  /**
   * Release exclusive ownership of lock.
   */
  void UnlockExclusive() { latch_.unlock(); }

  /**
   * Release shared ownership of lock.
   */
  void UnlockShared() { latch_.unlock_shared(); }

  /**
   * Scoped read latch that guarantees releasing the latch when destructed.
   */
  class ScopedSharedLatch {
   public:
    /**
     * Acquire write lock on ReaderWriterLatch.
     * @param rw_latch pointer to ReaderWriterLatch to acquire
     */
    explicit ScopedSharedLatch(SharedLatch *const rw_latch) : rw_latch_(rw_latch) { rw_latch_->LockShared(); }
    /**
     * Release write lock (if acquired).
     */
    ~ScopedSharedLatch() { rw_latch_->UnlockShared(); }
    DISALLOW_COPY_AND_MOVE(ScopedSharedLatch)

   private:
    SharedLatch *const rw_latch_;
  };

  /**
   * Scoped write latch that guarantees releasing the latch when destructed.
   */
  class ScopedExclusiveLatch {
   public:
    /**
     * Acquire read lock on ReaderWriterLatch.
     * @param rw_latch pointer to ReaderWriterLatch to acquire
     */
    explicit ScopedExclusiveLatch(SharedLatch *const rw_latch) : rw_latch_(rw_latch) { rw_latch_->LockExclusive(); }
    /**
     * Release read lock (if acquired).
     */
    ~ScopedExclusiveLatch() { rw_latch_->UnlockExclusive(); }
    DISALLOW_COPY_AND_MOVE(ScopedExclusiveLatch)
   private:
    SharedLatch *const rw_latch_;
  };

 private:
  std::shared_mutex latch_;
};

}  // namespace noisepage::common
