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
struct AlignedByteAllocator {
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
  void Reuse(T *reused) {}

  /**
   * Deletes the byte array.
   * @param ptr pointer to the byte array to be deleted.
   */
  void Delete(T *ptr) { delete[] ptr; }  // NOLINT
  // TODO(WAN): clang-tidy believes we are trying to free released memory.
  // We believe otherwise, hence we're telling it to shut up. We could be wrong though.
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
template <typename T, class Allocator = AlignedByteAllocator<T>>
class ObjectPool {
 public:
  /**
   * Initializes a new object pool with the supplied limit to the number of
   * objects reused.
   * @param size_limit the number of objects the object pool controls
   */
  explicit ObjectPool(uint64_t size_limit) : size_limit_(size_limit), current_size_(0) {}

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

  // TODO(Tianyu): The object pool can have much richer semantics in the future.
  // The current one does not do anything more intelligent other trying to keep
  // the memory it recycles to a limited size. (done)
  // A very clear improvement would be to bulk-malloc objects into the reuse queue,
  // or even to elastically grow or shrink the memory size depending on use pattern.

  /**
   * Returns a piece of memory to hold an object of T.
   *
   * It throws exception if the object pool fails to fetch memory.
   *
   * @return pointer to memory that can hold T
   */
  T *Get() {
    T *result = nullptr;
    if (!reuse_queue_.Dequeue(&result)) {
      uint64_t old = current_size_;
      while (old < size_limit_) {
        if (current_size_.compare_exchange_strong(old, old + 1)) {
          result = alloc_.New();
        } else {
          // CAS failed, current_size has changed. Recheck
          old = current_size_;
          continue;
        }
        break;
      }
      if (result == nullptr) {
        // out of memory
        throw std::bad_alloc();
      }
    } else {
      alloc_.Reuse(result);
    }
    return result;
  }

  /***
   * Set the object pool's size limit.
   *
   * @param new_size the new object pool size
   * @return true if new_size is successfully set and false otherwise
   */
  bool SetSizeLimit(uint64_t new_size) {
    if (new_size > current_size_) {
      size_limit_ = new_size;
      T *obj = nullptr;
      while (reuse_queue_.UnsafeSize() > size_limit_) {
        reuse_queue_.Dequeue(&obj);
        alloc_.Delete(obj);
      }
      return true;
    }
    return false;
  }

  /**
   * Releases the piece of memory given, allowing it to be freed or reused for
   * later. Although the memory is not necessarily immediately reclaimed, it will
   * be unsafe to access after entering this call.
   *
   * @param obj pointer to object to release
   */
  void Release(T *obj) {
    if (reuse_queue_.UnsafeSize() > size_limit_)
      alloc_.Delete(obj);
    else
      reuse_queue_.Enqueue(std::move(obj));
  }

 private:
  Allocator alloc_;
  ConcurrentQueue<T *> reuse_queue_;
  const uint64_t size_limit_;
  std::atomic<uint64_t> current_size_;
};
}  // namespace terrier::common
