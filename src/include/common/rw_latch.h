#pragma once

#include <tbb/reader_writer_lock.h>
#include "common/macros.h"

namespace terrier::common {

/**
 * A cheap and easy RW latch, currently wraps tbb::reader_writer_lock. From Intel's docs:
 *
 * A reader_writer_lock is scalable and nonrecursive. The implementation handles lock requests on a first-come
 * first-serve basis except that writers have preference over readers. Waiting threads busy wait, which can degrade
 * system performance if the wait is long. However, if the wait is typically short, a reader_writer_lock can provide
 * performance competitive with other mutexes.
 */
class ReaderWriterLatch {
 public:
  /**
   * Acquire write lock on mutex.
   */
  void Lock() { latch_.lock(); }

  /**
   * Acquire read lock on mutex.
   */
  void LockRead() { latch_.lock_read(); }

  /**
   * Try to acquire write lock on mutex.
   * @return true if lock acquired, false otherwise.
   */
  bool TryLock() { return latch_.try_lock(); }

  /**
   * Try to acquire read lock on mutex.
   * @return true if lock acquired, false otherwise.
   */
  bool TryLockRead() { return latch_.try_lock_read(); }

  /**
   * Release lock.
   */
  void Unlock() { latch_.unlock(); }

  /**
   * Scoped read latch that guarantees releasing the latch when destructed.
   */
  class ScopedReaderLatch {
   public:
    ScopedReaderLatch() = delete;
    /**
     * Acquire write lock on ReaderWriterLatch.
     * @param rw_latch pointer to ReaderWriterLatch to acquire
     */
    explicit ScopedReaderLatch(ReaderWriterLatch *rw_latch) : rw_latch_(rw_latch) { rw_latch_->LockRead(); }
    /**
     * Release write lock (if acquired).
     */
    ~ScopedReaderLatch() { rw_latch_->Unlock(); }
    DISALLOW_COPY_AND_MOVE(ScopedReaderLatch)
   private:
    ReaderWriterLatch *const rw_latch_;
  };

  /**
   * Scoped write latch that guarantees releasing the latch when destructed.
   */
  class ScopedWriterLatch {
   public:
    ScopedWriterLatch() = delete;
    /**
     * Acquire read lock on ReaderWriterLatch.
     * @param rw_latch pointer to ReaderWriterLatch to acquire
     */
    explicit ScopedWriterLatch(ReaderWriterLatch *rw_latch) : rw_latch_(rw_latch) { rw_latch_->Lock(); }
    /**
     * Release read lock (if acquired).
     */
    ~ScopedWriterLatch() { rw_latch_->Unlock(); }
    DISALLOW_COPY_AND_MOVE(ScopedWriterLatch)
   private:
    ReaderWriterLatch *const rw_latch_;
  };

 private:
  tbb::reader_writer_lock latch_;
};

}  // namespace terrier::common
