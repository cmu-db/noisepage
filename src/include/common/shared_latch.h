#pragma once

#include <shared_mutex>

#include "common/macros.h"

namespace noisepage::common {

/**
 * In order to be used with std::unique_lock, the adapted
 * type must meet the requirements of Lockable and BasicLockable
 */
template <typename Lockable>
class UniqueLockAdapter {
 public:
  /**
   * lock
   */
  void lock() {  // NOLINT
    auto &latch = static_cast<Lockable &>(*this);
    latch.LockExclusive();
  }
  /**
   * unlock
   */
  void unlock() {  // NOLINT
    auto &latch = static_cast<Lockable &>(*this);
    latch.UnlockExclusive();
  }
  /**
   * try lock
   */
  void try_lock() {  // NOLINT
    auto &latch = static_cast<Lockable &>(*this);
    latch.TryLockExclusive();
  }
};

/**
 * In order to be used with std::shared_lock, the adapted
 * type must meet the requirements of SharedMutex
 */
template <typename Lockable>
class SharedLockAdapter {
 public:
  /**
   * lock shared
   */
  void lock_shared() {  // NOLINT
    auto &latch = static_cast<Lockable &>(*this);
    latch.LockShared();
  }
  /**
   * unlock shared
   */
  void unlock_shared() {  // NOLINT
    auto &latch = static_cast<Lockable &>(*this);
    latch.UnlockShared();
  }
  /**
   * try lock shared
   */
  void try_lock_shared() {  // NOLINT
    auto &latch = static_cast<Lockable &>(*this);
    latch.TryLockShared();
  }
};

/**
 * A cheap(?) and easy shared (reader-writer) latch, currently wraps std::shared_mutex.
 */
class SharedLatch : public SharedLockAdapter<SharedLatch>, public UniqueLockAdapter<SharedLatch> {
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
     * Acquire read lock on ReaderWriterLatch.
     * @param rw_latch pointer to ReaderWriterLatch to acquire
     */
    explicit ScopedSharedLatch(SharedLatch *const rw_latch) : rw_latch_(rw_latch) { rw_latch_->LockShared(); }
    /**
     * Release read lock (if acquired).
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
     * Acquire write lock on ReaderWriterLatch.
     * @param rw_latch pointer to ReaderWriterLatch to acquire
     */
    explicit ScopedExclusiveLatch(SharedLatch *const rw_latch) : rw_latch_(rw_latch) { rw_latch_->LockExclusive(); }
    /**
     * Release write lock (if acquired).
     */
    ~ScopedExclusiveLatch() { rw_latch_->UnlockExclusive(); }
    DISALLOW_COPY_AND_MOVE(ScopedExclusiveLatch)
   private:
    SharedLatch *const rw_latch_;
  };

 private:
  std::shared_mutex latch_;
};

// In order to provide movable unique and shared latches we wrap C++ STL unique_lock and shared_lock

/**
 * exclusive movable write latch that guarantees releasing the latch when destructed.
 */
class UniqueLatch {
 public:
  /**
   * Acquire write latch on ReaderWriterLatch.
   * @param rw_latch pointer to ReaderWriterLatch to acquire
   */
  explicit UniqueLatch(SharedLatch *const rw_latch) : unique_lock_(*rw_latch) {}

 private:
  std::unique_lock<SharedLatch> unique_lock_;
};

/**
 * shared movable read latch that guarantees releasing the latch when destructed.
 */
class SharedLatchGuard {
 public:
  /**
   * Acquire read latch on ReaderWriterLatch.
   * @param rw_latch pointer to ReaderWriterLatch to acquire
   */
  explicit SharedLatchGuard(SharedLatch *const rw_latch) : shared_lock_(*rw_latch) {}

 private:
  std::shared_lock<SharedLatch> shared_lock_;
};

}  // namespace noisepage::common
