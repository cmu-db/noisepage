#pragma once

#include <algorithm>
#include <atomic>
#include <memory>
#include <vector>

#include "execution/util/common.h"
#include "execution/util/macros.h"
#include "execution/util/spin_latch.h"

namespace terrier::execution::sql {

class MemoryTracker;

/**
 * A memory pool
 */
class MemoryPool {
 public:
  /**
   * Create a pool that reports to the given memory tracker @em tracker.
   * @param tracker The tracker to use to report allocations.
   */
  explicit MemoryPool(MemoryTracker *tracker);

  /**
   * This class cannot be copied or moved.
   */
  DISALLOW_COPY_AND_MOVE(MemoryPool);

  /**
   * Allocate @em size bytes of memory from this pool.
   * @param size The number of bytes to allocate.
   * @param clear Whether to clear the bytes before returning.
   * @return A pointer to at least @em size bytes of memory.
   */
  void *Allocate(std::size_t size, bool clear);

  /**
   * Allocate @em size bytes of memory from this pool with a specific alignment.
   * @param size The number of bytes to allocate.
   * @param alignment The desired alignment of the allocation.
   * @param clear Whether to clear the bytes before returning.
   * @return A pointer to at least @em size bytes of memory.
   */
  void *AllocateAligned(std::size_t size, std::size_t alignment, bool clear);

  /**
   * Allocate an array of elements with a specific alignment different than the
   * alignment of type @em T. The provided alignment must be larger than or
   * equal to the alignment required for T.
   * @tparam T The type of each element in the array.
   * @param num_elems The number of elements in the array.
   * @param alignment The alignment of the allocated array
   * @param clear Flag to zero-out the contents of the array before returning.
   * @return @return An array pointer to at least @em num_elems elements of type
   * @em T
   */
  template <typename T>
  T *AllocateArray(std::size_t num_elems, std::size_t alignment, bool clear) {
    alignment = std::max(alignof(T), alignment);
    return reinterpret_cast<T *>(AllocateAligned(sizeof(T) * num_elems, alignment, clear));
  }

  /**
   * Allocate a contiguous array of elements of the given type from this pool.
   * The alignment of the array will be the alignment of the type @em T.
   * @tparam T The type of each element in the array.
   * @param num_elems The number of requested elements in the array.
   * @param clear Flag to zero-out the contents of the array before returning.
   * @return An array pointer to at least @em num_elems elements of type @em T
   */
  template <typename T>
  T *AllocateArray(std::size_t num_elems, bool clear) {
    return AllocateArray<T>(num_elems, alignof(T), clear);
  }

  /**
   * Deallocate memory allocated by this pool.
   * @param ptr The pointer to the memory.
   * @param size The size of the allocation.
   */
  void Deallocate(void *ptr, std::size_t size);

  /**
   * Deallocate an array allocated by this pool.
   * @tparam T The type of the element in the array.
   * @param ptr The pointer to the array.
   * @param num_elems The number of elements in the array at the time is was
   *                  allocated.
   */
  template <typename T>
  void DeallocateArray(T *const ptr, const std::size_t num_elems) {
    Deallocate(ptr, sizeof(T) * num_elems);
  }

  /**
   * Set the global threshold for when to use MMap and huge pages versus
   * standard allocations.
   * @param size The size threshold.
   */
  static void SetMMapSizeThreshold(std::size_t size);

  /**
   * Get the tracker
   */
  MemoryTracker *GetTracker() { return tracker_; }

 private:
  // Metadata tracker for memory allocations
  MemoryTracker *tracker_;

  //
  static std::atomic<u64> kMmapThreshold;
};

/**
 * An STL-compliant allocator that uses an injected memory pool
 * @tparam T The types of elements this allocator handles
 */
template <typename T>
class MemoryPoolAllocator {
 public:
  /**
   * Type of values
   */
  using value_type = T;

  /**
   * Constructor
   * @param memory memory pool to use for allocation
   */
  MemoryPoolAllocator(MemoryPool *memory) : memory_(memory) {}  // NOLINT

  /**
   * Copy constructor
   * @param other memory pool to copy
   */
  MemoryPoolAllocator(const MemoryPoolAllocator &other) : memory_(other.memory_) {}

  /**
   * Copy constructor
   * @tparam U type of the other pool's values
   * @param other memory pool to copy
   */
  template <typename U>
  MemoryPoolAllocator(const MemoryPoolAllocator<U> &other)  // NOLINT
      : memory_(other.memory_) {}

  template <typename U>
  friend class MemoryPoolAllocator;

  /**
   * Allocates an array of the given size for the given type
   * @param n size of the array
   * @return an array of the given size
   */
  T *allocate(std::size_t n) { return memory_->AllocateArray<T>(n, false); }

  /**
   * Deallocates an array
   * @param ptr array to deallocate
   * @param n size of the array
   */
  void deallocate(T *ptr, std::size_t n) { memory_->Deallocate(ptr, n); }

  /**
   * Equality comparison for two memory pools
   */
  bool operator==(const MemoryPoolAllocator &other) const { return memory_ == other.memory_; }

  /**
   * Unequality comparison for two memory pools
   */
  bool operator!=(const MemoryPoolAllocator &other) const { return !(*this == other); }

 private:
  MemoryPool *memory_;
};

/**
 * std vector using the memory pool as its allocator
 * @tparam T type of the elements
 */
template <typename T>
class MemPoolVector : public std::vector<T, MemoryPoolAllocator<T>> {
  /**
   * Base std vector type
   */
  using BaseType = std::vector<T, MemoryPoolAllocator<T>>;

 public:
  /**
   * Empty Constructor
   * @param memory memory pool to use for allocation
   */
  explicit MemPoolVector(MemoryPool *memory) : BaseType(MemoryPoolAllocator<T>(memory)) {}

  /**
   * Constructor with initial size
   * @param n initial size
   * @param memory memory pool to use for allocation
   */
  MemPoolVector(std::size_t n, MemoryPool *memory) : BaseType(n, MemoryPoolAllocator<T>(memory)) {}

  /**
   * Constructor with initial size and value
   * @param n initial size
   * @param elem initial value
   * @param memory memory pool to use for allocation
   */
  MemPoolVector(std::size_t n, const T &elem, MemoryPool *memory) : BaseType(n, elem, MemoryPoolAllocator<T>(memory)) {}

  /**
   * Constructor with an initializer list
   * @param list list of initial elements
   * @param memory memory pool to use for allocation
   */
  MemPoolVector(std::initializer_list<T> list, MemoryPool *memory) : BaseType(list, MemoryPoolAllocator<T>(memory)) {}

  /**
   * Constructor to copy from another iterator
   * @tparam InputIter type of the other iterator
   * @param first beginning of the other iterator
   * @param last end of the other iterator
   * @param memory memory pool to use for allocation
   */
  template <typename InputIter>
  MemPoolVector(InputIter first, InputIter last, MemoryPool *memory)
      : BaseType(first, last, MemoryPoolAllocator<T>(memory)) {}
};

}  // namespace terrier::execution::sql
