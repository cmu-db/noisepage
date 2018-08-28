#pragma once

#include <memory>
#include <utility>
#include "common/allocator.h"
#include "common/container/concurrent_queue.h"
#include "common/stats/object_pool_stats.h"
#include "common/typedefs.h"

namespace terrier::common {
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
   * @param reuse_limit
   */
  explicit ObjectPool(const uint64_t reuse_limit) : reuse_limit_(reuse_limit) {}

  /**
   * Initializes a new object pool with the supplied limit to the number of
   * objects reused with statistics collector.
   * @param reuse_limit
   * @param stats_collector  stats collector pointer to collect and to print stats in this class.
   */
  explicit ObjectPool(uint64_t reuse_limit, StatsCollector *stats_collector) : reuse_limit_(reuse_limit) {
    if (stats_collector != nullptr) {
      // enable stats collection
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
  const uint64_t reuse_limit_;

  // statistics about usage of blocks in the class.
  // enable_stats is used to disable the statistics.
  bool enable_stats_ = false;
  std::unique_ptr<ObjectPoolStats> stats_;
};
}  // namespace terrier::common
