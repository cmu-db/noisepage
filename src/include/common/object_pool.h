#pragma once

#include <string>
#include <utility>
#include "common/container/concurrent_queue.h"
#include "common/stats/object_pool_stats.h"
#include "common/typedefs.h"

namespace terrier::common {
/**
 * Allocator that allocates and destroys a byte array. Memory location returned by this default allocator is
 * not zeroed-out.
 * @tparam T object whose size determines the byte array size.
 */
template <typename T>
struct ByteAllocator {
  /**
   * Allocates a new byte array sized to hold a T.
   * @return a pointer to the byte array allocated.
   */
  T *New() {
    auto *result = reinterpret_cast<T *>(new byte[sizeof(T)]);
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
   * Initializes a new object pool with the supplied limit to the number of
   * objects reused with statistics collector.
   * @param reuse_limit
   * @param stats_collector  stats collector pointer to collect and to print stats in this class.
   */
  explicit ObjectPool(uint64_t reuse_limit, StatsCollector *stats_collector) : reuse_limit_(reuse_limit) {
    if (stats_collector != nullptr) {
      enable_stats_ = true;
      stats_ = std::make_unique<ObjectPoolStats>(stats_collector);
    }
  }

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
  // the memory it recycles to a limited size.
  // A very clear improvement would be to bulk-malloc objects into the reuse queue,
  // or even to elastically grow or shrink the memory size depending on use pattern.

  /**
   * Returns a piece of memory to hold an object of T.
   *
   * @return pointer to memory that can hold T
   */
  T *Get() {
    T *result = nullptr;
    if (!reuse_queue_.Dequeue(&result)) {
      result = alloc_.New();

      // for statistics
      if (enable_stats_) stats_->IncrementCreateBlockCounter();
    } else {
      alloc_.Reuse(result);

      // for statistics
      if (enable_stats_) stats_->IncrementReuseBlockCounter();
    }
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
    if (reuse_queue_.UnsafeSize() > reuse_limit_) {
      alloc_.Delete(obj);
    } else {
      reuse_queue_.Enqueue(std::move(obj));
    }
  }

 private:
  Allocator alloc_;
  ConcurrentQueue<T *> reuse_queue_;
  // TODO(Tianyu): It might make sense for this to be changeable in the future
  const uint64_t reuse_limit_;

  // statistics about usage of blocks in the class.
  // boolean flag, enable_stats, is used to disable the statistics.
  bool enable_stats_ = false;
  std::unique_ptr<ObjectPoolStats> stats_;
};
}  // namespace terrier::common
