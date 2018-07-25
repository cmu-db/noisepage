#pragma once

#include "common/concurrent_queue.h"
#include "typedefs.h"
namespace terrier {
template <typename T>
struct ByteAllocator {
  T *New() { return reinterpret_cast<T *>(new byte[sizeof(T)]); }

  void Delete(T *ptr) { delete[] ptr; }
};

template <typename T>
struct DefaultConstructorAllocator {
  T *New() { return new T(); }

  void Delete(T *ptr) { delete ptr; }
};

// TODO(Tianyu): Should this be by size or by class type?
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
template <typename T, class Allocator = ByteAllocator<T>>
class ObjectPool {
 public:
  /**
   * Initializes a new object pool with the supplied limit to the number of
   * objects reused.
   * @param reuse_limit
   */
  explicit ObjectPool(uint64_t reuse_limit) : reuse_limit_(reuse_limit) {}

  /**
   * Destructs the memory pool. Frees any memory it holds.
   *
   * Beware that the object pool will not deallocate some piece of memory
   * not explicitly released via a Release call.
   */
  FAKED_IN_TEST ~ObjectPool() {
    T *result;
    while (reuse_queue_.Dequeue(result)) alloc_.Delete(result);
  }

  // TODO(Tianyu): The object pool can have much richer semantics in the future.
  // The current one does not do anything more intelligent other trying to keep
  // the memory it recycles to a limited size.
  // A very clear improvement would be to bulk-malloc objects into the reuse queue,
  // or even to elastically grow or shrink the memory size depending on use pattern.

  /**
   * Returns a piece of memory to hold an object of T. The memory is always
   * 0-initialized.
   *
   * @return pointer to memory that can hold T
   */
  FAKED_IN_TEST T *Get() {
    T *result;
    if (!reuse_queue_.Dequeue(result)) result = alloc_.New();
    PELOTON_MEMSET(result, 0, sizeof(T));
    return result;
  }

  /**
   * Releases the piece of memory given, allowing it to be freed or reused for
   * later. Although the memory is not necessarily immediately reclaimed, it will
   * be unsafe to access after entering this call.
   *
   * @param obj pointer to object to release
   */
  FAKED_IN_TEST void Release(T *obj) {
    if (reuse_queue_.UnsafeSize() > reuse_limit_)
      alloc_.Delete(obj);
    else
      reuse_queue_.Enqueue(std::move(obj));
  }

 private:
  Allocator alloc_;
  ConcurrentQueue<T *> reuse_queue_;
  // TODO(Tianyu): It might make sense for this to be changeable in the future
  const uint64_t reuse_limit_;
};
}  // namespace terrier
