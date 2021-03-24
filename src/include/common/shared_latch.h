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

  /**
   * Unique read latch that guarantees releasing the latch when destructed.
   */
  class UniqueSharedLatch {
   public:
    /**
     * Acquire read lock on ReaderWriterLatch.
     * @param rw_latch pointer to ReaderWriterLatch to acquire
     */
    explicit UniqueSharedLatch(SharedLatch *const rw_latch) : rw_latch_(rw_latch), owns_(false) {
      rw_latch_->LockShared();
      owns_ = true;
    }
    /**
     * Release read lock (if acquired and owned).
     */
    ~UniqueSharedLatch() {
      if (owns_) {
        rw_latch_->UnlockShared();
      }
    }
    /**
     * Moves owner of other UniqueSharedLatch to this
     * @param other UniqueSharedLatch to move ownership from
     */
    UniqueSharedLatch(UniqueSharedLatch &&other) noexcept : rw_latch_(other.rw_latch_), owns_(other.owns_) {
      other.owns_ = false;
    }
    /*
     * Move assignment is currently unimplemented because it's not needed yet
     */
    UniqueSharedLatch &operator=(UniqueSharedLatch &&other) = delete;
    DISALLOW_COPY(UniqueSharedLatch)

   private:
    SharedLatch *const rw_latch_;
    bool owns_;
  };

  /**
   * Unique write latch that guarantees releasing the latch when destructed.
   */
  class UniqueExclusiveLatch {
   public:
    /**
     * Acquire read lock on ReaderWriterLatch.
     * @param rw_latch pointer to ReaderWriterLatch to acquire
     */
    explicit UniqueExclusiveLatch(SharedLatch *const rw_latch) : rw_latch_(rw_latch), owns_(false) {
      rw_latch_->LockExclusive();
      owns_ = true;
    }
    /**
     * Release read lock (if acquired and owned).
     */
    ~UniqueExclusiveLatch() {
      if (owns_) {
        rw_latch_->UnlockExclusive();
      }
    }
    /**
     * Moves owner of other UniqueExclusiveLatch to this
     * @param other UniqueExclusiveLatch to move ownership from
     */
    UniqueExclusiveLatch(UniqueExclusiveLatch &&other) noexcept : rw_latch_(other.rw_latch_), owns_(other.owns_) {
      other.owns_ = false;
    }
    /*
     * Move assignment is currently unimplemented because it's not needed yet
     */
    UniqueExclusiveLatch &operator=(UniqueExclusiveLatch &&other) = delete;
    DISALLOW_COPY(UniqueExclusiveLatch)

   private:
    SharedLatch *const rw_latch_;
    bool owns_;
  };

 private:
  std::shared_mutex latch_;
};

}  // namespace noisepage::common
