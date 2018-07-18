#pragma once

#include "common/concurrent_queue.h"
#include "common_defs.h"
namespace terrier {

// TODO(Tianyu): Should this be by size or by class type?
/**
 * Object pool for memory allocation.
 *
 * This prevents liberal calls to malloc and new in the code and makes tracking
 * our memory performance easier.
 * @tparam T the type of objects in the pool.
 */
template <typename T>
class ObjectPool {
 public:
  /**
   * Initializes a new object pool with the supplied limit to the number of
   * objects reused.
   * @param reuse_limit
   */
  explicit ObjectPool(uint64_t reuse_limit) : reuse_limit_(reuse_limit) {}

  virtual /**
   * Destructs the memory pool. Frees any memory it holds.
   *
   * Beware that the object pool will not deallocate some piece of memory
   * not explicitly released via a Release call.
   */
  ~ObjectPool() {
    T *result;
    while (reuse_queue_.Dequeue(result))
      delete result;
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
  virtual T *Get() {
    T *result;
    if (!reuse_queue_.Dequeue(result))
      result = reinterpret_cast<T *>(new byte[sizeof(T)]);
    PELOTON_MEMSET(result, 0, sizeof(T));
    return result;
  }

  virtual /**
   * Releases the piece of memory given, allowing it to be freed or reused for
   * later. Although the memory is not necessarily immediately reclaimed, it will
   * be unsafe to access after entering this call.
   *
   * @param obj pointer to object to release
   */
  void Release(T *obj) {
    if (reuse_queue_.UnsafeSize() > reuse_limit_)
      delete obj;
    else
      reuse_queue_.Enqueue(std::move(obj));
  }

  virtual size_t ObjectSize() {
    return sizeof(T);
  }

 private:
  ConcurrentQueue<T *> reuse_queue_;
  // TODO(Tianyu): It might make sense for this to be changeable in the future
  const uint64_t reuse_limit_;
};
}
