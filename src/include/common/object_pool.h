#pragma once

#include <utility>
#include "common/container/concurrent_queue.h"
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
    auto *result = reinterpret_cast<T *>(new uint64_t[sizeof(T) / 8 + 1]);
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
  /**
   * Initializes a new object pool with the supplied limit to the number of
   * objects reused.
   * @param reuse_limit
   */
  explicit ObjectPool(const uint64_t reuse_limit) : reuse_limit_(reuse_limit) {}

  /**
   * Destructs the memory pool. Frees any memory it holds.
   *
   * Beware that the object pool will not deallocate some piece of memory
   * not explicitly released via a Release call.
   */
  ~ObjectPool() {
    T *result = nullptr;
    while (reuse_queue_.Dequeue(&result)) alloc_.Delete(result);
  }

  /**
   * Returns a piece of memory to hold an object of T.
   *
   * @return pointer to memory that can hold T
   */
  T *Get() {
    T *result = nullptr;
    if (!reuse_queue_.Dequeue(&result))
      result = alloc_.New();
    else
      alloc_.Reuse(result);
    return result;
  }

  /**
   * Releases the piece of memory given, allowing it to be freed or reused for
   * later. Although the memory is not necessarily immediately reclaimed, it will
   * be unsafe to access after entering this call.
   *
   * @param obj pointer to object to release
   */
  void Release(T *obj) {
    if (reuse_queue_.UnsafeSize() > reuse_limit_)
      alloc_.Delete(obj);
    else
      reuse_queue_.Enqueue(std::move(obj));
  }

 private:
  Allocator alloc_;
  ConcurrentQueue<T *> reuse_queue_;
  const uint64_t reuse_limit_;
};
}  // namespace terrier::common
