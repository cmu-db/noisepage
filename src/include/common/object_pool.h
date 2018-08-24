#pragma once

#include <queue>
#include <utility>
#include "common/container/concurrent_queue.h"
#include "common/spin_latch.h"
#include "common/typedefs.h"

namespace terrier::common {
/**
 * Allocator that allocates and destroys a byte array. Memory location returned by this default allocator is
 * not zeroed-out. The address returned is guaranteed to be aligned to 8 bytes.
 * @tparam T object whose size determines the byte array size.
 */
template <typename T>
class AlignedByteAllocator {
 public:
  /**
   * Allocates a new byte array sized to hold a T.
   * @return a pointer to the byte array allocated.
   */
  T *New() {
    auto *result = reinterpret_cast<T *>(new uint64_t[(sizeof(T) + 7) / 8]);
    Reuse(result);
    return result;
  }

  /**
   * Reuse a reused chunk of memory to be handed out again
   * @param reused memory location, possibly filled with junk bytes
   */
  void Reuse(T *const reused) {}

  /**
   * Deletes the byte array.
   * @param ptr pointer to the byte array to be deleted.
   */
  void Delete(T *const ptr) { delete[] ptr; }  // NOLINT
  // clang-tidy believes we are trying to free released memory.
  // We believe otherwise, hence we're telling it to shut up.
};

// TODO(Yangjun): this class should be moved somewhere else.
/***
 * An exception thrown by object pools when they reach their size limits and
 * cannot give more memory space for objects.
 */
class NoMoreObjectException : public std::exception {
 public:
  explicit NoMoreObjectException(uint64_t limit) : limit_(limit) {}
  /***
   * Describe the exception.
   * @return a string of exception description
   */
  const char *what() const noexcept override {
    return ("Object Pool have no object to hand out. Exceed size limit " + std::to_string(limit_) + ".\n").c_str();
  }

 private:
  uint64_t limit_;
};

/**
 * Object pool for memory allocation.
 *
 * This prevents liberal calls to malloc and new in the code and makes tracking
 * our memory performance easier.
 * @tparam T the type of objects in the pool.
 * @tparam The allocator to use when constructing and destructing a new object.
 *         In most cases it can be left out and the default allocator will
 *         suffice (malloc). If you want richer behavior, define your own
 *         structure to return a pointer that the object pool will then take
 *         control over. The returned pointer will be eventually freed with the
 *         supplied Delete method, but its memory location will potentially be
 *         handed out multiple times before that happens.
 */
template <typename T, class Allocator = AlignedByteAllocator<T>>
class ObjectPool {
 public:
  /***
   * Initializes a new object pool with the supplied limit to the number of
   * objects reused.
   *
   * @param size_limit the maximum number of objects the object pool controls
   * @param reuse_limit the maximum number of reusable objects
   */
  ObjectPool(const uint64_t size_limit, const uint64_t reuse_limit)
      : size_limit_(size_limit), reuse_limit_(reuse_limit), current_size_(0) {}

  /**
   * Initializes a new object pool with the supplied limit to the number of
   * objects reused. The reuse_limit is set to be the same as size limit.
   *
   * @param size_limit the number of objects the object pool controls
   */
  explicit ObjectPool(const uint64_t size_limit)
      : size_limit_(size_limit), reuse_limit_(size_limit), current_size_(0) {}

  /**
   * Destructs the memory pool. Frees any memory it holds.
   *
   * Beware that the object pool will not deallocate some piece of memory
   * not explicitly released via a Release call.
   */
  ~ObjectPool() {
    T *result = nullptr;
    while (!reuse_queue_.empty()) {
      result = reuse_queue_.front();
      alloc_.Delete(result);
      reuse_queue_.pop();
    }
  }

  /**
   * Returns a piece of memory to hold an object of T.
   * @throw NoMoreObjectException if the object pool fails to fetch memory.
   * @return pointer to memory that can hold T
   */
  T *Get() {
    SpinLatch::ScopedSpinLatch guard(&latch_);
    T *result = nullptr;
    if (reuse_queue_.empty()) {
      if (current_size_ < size_limit_) result = alloc_.New();
      if (result == nullptr) {
        // out of memory
        throw NoMoreObjectException(size_limit_);
      }
      current_size_++;
    } else {
      result = reuse_queue_.front();
      reuse_queue_.pop();
      alloc_.Reuse(result);
    }
    TERRIER_ASSERT(current_size_ <= size_limit_, "object pool size exceed it's size limit");
    return result;
  }

  /**
   * Set the object pool's size limit.
   *
   * The operation fails if the object pool has already allocated more objects
   * than the size limit.
   *
   * @param new_size the new object pool size
   * @return true if new_size is successfully set and false the operation fails
   */
  bool SetSizeLimit(uint64_t new_size) {
    SpinLatch::ScopedSpinLatch guard(&latch_);
    if (new_size >= current_size_) {
      // current_size_ might increase and become > new_size if we don't use lock
      size_limit_ = new_size;
      TERRIER_ASSERT(current_size_ <= size_limit_, "object pool size exceed it's size limit");
      return true;
    }
    return false;
  }

  /**
   * Set the reuse limit to a new value.
   *
   * @param new_reuse_limit
   */
  void SetReuseLimit(uint64_t new_reuse_limit) {
    SpinLatch::ScopedSpinLatch guard(&latch_);
    reuse_limit_ = new_reuse_limit;
    T *obj = nullptr;
    while (reuse_queue_.size() > reuse_limit_) {
      obj = reuse_queue_.front();
      alloc_.Delete(obj);
      reuse_queue_.pop();
      current_size_--;
    }
  }

  /**
   * Releases the piece of memory given, allowing it to be freed or reused for
   * later. Although the memory is not necessarily immediately reclaimed, it will
   * be unsafe to access after entering this call.
   *
   * @param obj pointer to object to release
   */
  void Release(T *obj) {
    SpinLatch::ScopedSpinLatch guard(&latch_);
    if (reuse_queue_.size() >= reuse_limit_) {
      alloc_.Delete(obj);
      current_size_--;
    } else {
      reuse_queue_.push(obj);
    }
  }

 private:
  Allocator alloc_;
  SpinLatch latch_;
  std::queue<T *> reuse_queue_;
  uint64_t size_limit_;   // the maximum number of objects a object pool can have
  uint64_t reuse_limit_;  // the maximum number of reusable objects in reuse_queue
  // current_size_ represents the number of objects the object pool has allocated,
  // including objects that have been given out to callers and those reside in reuse_queue
  uint64_t current_size_;
};
}  // namespace terrier::common
