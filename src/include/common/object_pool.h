#pragma once

#include <queue>
#include <string>
#include <utility>

#include "common/allocator.h"
#include "common/spin_latch.h"
#include "common/strong_typedef.h"

namespace terrier::common {
// TODO(Yangjun): this class should be moved somewhere else.
/**
 * An exception thrown by object pools when they reach their size limits and
 * cannot give more memory space for objects.
 */
class NoMoreObjectException : public std::exception {
 public:
  /**
   * Construct an exception that can be thrown by a object pool
   * @param limit the object pool limit size
   */
  explicit NoMoreObjectException(uint64_t limit)
      : message_("Object Pool have no object to hand out. Exceed size limit " + std::to_string(limit) + ".\n") {}
  /**
   * Describe the exception.
   * @return a string of exception description
   */
  const char *what() const noexcept override { return message_.c_str(); }

 private:
  std::string message_;
};
/**
 * An exception thrown by object pools when the allocator fails to fetch memory
 * space. This can happen when the caller asks for an object, the object pool
 * doesn't have reusable object and the underlying allocator fails to get new
 * memory due to system running out of memory
 */
class AllocatorFailureException : public std::exception {
 public:
  /**
   * Describe the exception.
   * @return a string of exception description
   */
  const char *what() const noexcept override { return "Allocator fails to allocate memory.\n"; }
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
template <typename T, class Allocator = ByteAlignedAllocator<T>>
class ObjectPool {
 public:
  /**
   * Initializes a new object pool with the supplied limit to the number of
   * objects reused.
   *
   * @param size_limit the maximum number of objects the object pool controls
   * @param reuse_limit the maximum number of reusable objects
   */
  ObjectPool(uint64_t size_limit, uint64_t reuse_limit)
      : size_limit_(size_limit), reuse_limit_(reuse_limit), current_size_(0) {}

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
   * @throw NoMoreObjectException if the object pool has reached the limit of how many objects it may hand out.
   * @throw AllocatorFailureException if the allocator fails to return a valid memory address.
   * @return pointer to memory that can hold T
   */
  T *Get() {
    SpinLatch::ScopedSpinLatch guard(&latch_);
    if (reuse_queue_.empty() && current_size_ >= size_limit_) throw NoMoreObjectException(size_limit_);
    T *result = nullptr;
    if (reuse_queue_.empty()) {
      result = alloc_.New();  // result could be null because the allocator may not find enough memory space
      if (result != nullptr) current_size_++;
    } else {
      result = reuse_queue_.front();
      reuse_queue_.pop();
      alloc_.Reuse(result);
    }
    // If result is nullptr. The call to alloc_.New() failed (i.e. can't allocate more memory from the system).
    if (result == nullptr) throw AllocatorFailureException();
    TERRIER_ASSERT(current_size_ <= size_limit_, "Object pool has exceeded its size limit.");
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
      TERRIER_ASSERT(current_size_ <= size_limit_, "object pool size exceed its size limit");
      return true;
    }
    return false;
  }

  /**
   * Set the reuse limit to a new value. This function always succeed and immediately changes
   * reuse limit.
   *
   * A reuse limit simply determines the maximum number of reusable objects the object pool should
   * maintain and can be any non-negative number.
   *
   * If reuse limit > size limit. It's still valid.
   * It's just that the number of reusable objects in the pool will never reach reuse limit because
   * # of reusable objects <= current size <= size limit < reuse_limit.
   *
   * If it's 0, then the object pool just never reuse object.
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
    TERRIER_ASSERT(obj != nullptr, "releasing a null pointer");
    SpinLatch::ScopedSpinLatch guard(&latch_);
    if (reuse_queue_.size() >= reuse_limit_) {
      alloc_.Delete(obj);
      current_size_--;
    } else {
      reuse_queue_.push(obj);
    }
  }

  /**
   * @return size limit of the object pool
   */
  uint64_t GetSizeLimit() const { return size_limit_; }

 private:
  Allocator alloc_;
  SpinLatch latch_;
  // TODO(yangjuns): We don't need to reuse objects in a FIFO pattern. We could potentially pass a second template
  // parameter to define the backing container for the std::queue. That way we can measure each backing container.
  std::queue<T *> reuse_queue_;
  uint64_t size_limit_;   // the maximum number of objects a object pool can have
  uint64_t reuse_limit_;  // the maximum number of reusable objects in reuse_queue
  // current_size_ represents the number of objects the object pool has allocated,
  // including objects that have been given out to callers and those reside in reuse_queue
  uint64_t current_size_;
};
}  // namespace terrier::common
