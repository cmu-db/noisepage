#pragma once

#include <algorithm>
#include <atomic>
#include <memory>
#include <utility>
#include <vector>

#include "common/macros.h"
#include "common/managed_pointer.h"
#include "common/spin_latch.h"
#include "execution/util/execution_common.h"

namespace noisepage::execution::sql {

class MemoryTracker;

/**
 * A thin wrapper around a pointer to an object allocated from a memory pool.
 * Exists to:
 * 1. alert users that this object has to be returned to the pool at some point.
 * 2. ensure that users don't try to delete objects allocated from pool (if they were returned as raw pointers).
 * @tparam T The type of the pointed-to object.
 */
template <typename T>
class MemPoolPtr {
 public:
  /**
   * Null pointer.
   */
  MemPoolPtr() : obj_(nullptr) {}

  /** Construct a memory pool. */
  explicit MemPoolPtr(std::nullptr_t) : obj_(nullptr) {}

  /**
   * Copy.
   */
  MemPoolPtr(const MemPoolPtr<T> &ptr) : obj_(ptr.obj_) {}

  /**
   * Move.
   */
  MemPoolPtr(MemPoolPtr<T> &&ptr) noexcept : obj_(ptr.obj_) { ptr.obj_ = nullptr; }

  /**
   * Arrow operation.
   */
  T *operator->() { return obj_; }

  /**
   * Arrow operation.
   */
  const T *operator->() const { return obj_; }

  /**
   * Move assignment to force users to move objects rather than copy.
   */
  MemPoolPtr<T> &operator=(MemPoolPtr<T> &&that) noexcept {
    obj_ = that.obj_;
    that.obj_ = nullptr;
    return *this;
  }

  /**
   * Raw access.
   */
  T *Get() { return obj_; }

  /**
   * Raw access.
   */
  const T *Get() const { return obj_; }

  /**
   * Does this pointer point to anything?
   */
  explicit operator bool() const { return obj_; }

 private:
  friend class MemoryPool;

  // Creating from raw pointers only allowed by MemoryPool
  explicit MemPoolPtr(T *obj) : obj_(obj) {}

 private:
  T *obj_;
};

template <typename T1, typename T2>
inline bool operator==(const MemPoolPtr<T1> &a, const MemPoolPtr<T2> &b) {
  return a.Get() == b.Get();
}

template <typename T>
inline bool operator==(std::nullptr_t, const MemPoolPtr<T> &b) noexcept {
  return !b;
}

template <typename T>
inline bool operator==(const MemPoolPtr<T> &a, std::nullptr_t) noexcept {
  return !a;
}

template <typename T1, typename T2>
inline bool operator!=(const MemPoolPtr<T1> &a, const MemPoolPtr<T2> &b) {
  return a.Get() != b.Get();
}

template <typename T>
inline bool operator!=(std::nullptr_t, const MemPoolPtr<T> &a) noexcept {
  return static_cast<bool>(a);
}

template <typename T>
inline bool operator!=(const MemPoolPtr<T> &a, std::nullptr_t) noexcept {
  return static_cast<bool>(a);
}

/**
 * A memory pool
 */
class EXPORT MemoryPool {
 public:
  /**
   * Create a pool that reports to the given memory tracker @em tracker.
   * @param tracker The tracker to use to report allocations.
   */
  explicit MemoryPool(common::ManagedPointer<sql::MemoryTracker> tracker);

  /**
   * This class cannot be copied or moved.
   */
  DISALLOW_COPY_AND_MOVE(MemoryPool);

  /**
   * Allocate @em size bytes of memory from this pool. The returned memory is not initialized.
   * @param size The number of bytes to allocate.
   * @return A pointer to at least @em size bytes of uninitialized memory.
   */
  void *Allocate(std::size_t size) { return Allocate(size, false); }

  /**
   * Allocate @em size bytes of memory from this pool.
   * If @em clear is set, the memory chunk is zeroed out before returning.
   *
   * @param size The number of bytes to allocate.
   * @param clear Whether to clear the bytes before returning.
   * @return A pointer to at least @em size bytes of memory.
   */
  void *Allocate(std::size_t size, bool clear) { return AllocateAligned(size, 0, clear); }

  /**
   * Allocate @em size bytes of memory from this pool. The returned memory is not initialized.
   * @param size The number of bytes to allocate.
   * @param alignment The alignment of the bytes to be allocated.
   * @return A pointer to at least @em size bytes of uninitialized memory.
   */
  void *AllocateAligned(std::size_t size, std::size_t alignment) { return AllocateAligned(size, alignment, false); }

  /**
   * Allocate @em size bytes of memory from this pool with a specific alignment.
   * If @em alignment is less than the default minimum alignment of 8-bytes, a standard allocation is performed.
   * If @em clear is set, the memory chunk is zeroed out before returning.
   *
   * @param size The number of bytes to allocate.
   * @param alignment The desired alignment of the allocation.
   * @param clear Whether to clear the bytes before returning.
   * @return A pointer to at least @em size bytes of memory.
   */
  void *AllocateAligned(std::size_t size, std::size_t alignment, bool clear);

  /**
   * Allocate an array of elements with a specific alignment different than the alignment of type @em T.
   * The provided alignment must be larger than or equal to the alignment required for T.
   *
   * @pre The requested alignment must be larger than or equal to the alignment of the type @em T.
   *
   * @tparam T The type of each element in the array.
   * @param num_elems The number of elements in the array.
   * @param alignment The alignment of the allocated array
   * @param clear Flag to zero-out the contents of the array before returning.
   * @return An array pointer to at least @em num_elems elements of type @em T.
   */
  template <typename T>
  T *AllocateArray(const std::size_t num_elems, std::size_t alignment, const bool clear) {
    alignment = std::max(alignof(T), alignment);
    return reinterpret_cast<T *>(AllocateAligned(sizeof(T) * num_elems, alignment, clear));
  }

  /**
   * Allocate a contiguous array of elements of the given type from this pool.
   * The alignment of the array will be the alignment of the type @em T.
   * @tparam T The type of each element in the array.
   * @param num_elems The number of requested elements in the array.
   * @param clear Flag to zero-out the contents of the array before returning.
   * @return An array pointer to at least @em num_elems elements of type @em T.
   */
  template <typename T>
  T *AllocateArray(const std::size_t num_elems, const bool clear) {
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
   * @param num_elems The number of elements in the array at the time it was allocated.
   */
  template <typename T>
  void DeallocateArray(T *const ptr, const std::size_t num_elems) {
    Deallocate(ptr, sizeof(T) * num_elems);
  }

  // -------------------------------------------------------
  // Object allocations
  // -------------------------------------------------------

  /**
   * Allocate an object from from this pool.
   * @tparam T The type of the object to allocate.
   * @tparam Args The argument types.
   * @param args The arguments.
   * @return A construct object allocated from this pool.
   */
  template <typename T, typename... Args>
  MemPoolPtr<T> MakeObject(Args &&... args) {
    auto *object_mem = Allocate(sizeof(T), true);
    auto *obj = new (object_mem) T(std::forward<Args>(args)...);
    return MemPoolPtr<T>(obj);
  }

  /**
   * Free an object allocated from this pool.
   * @tparam T The type of the object.
   * @param object The object.
   */
  template <typename T>
  void DeleteObject(MemPoolPtr<T> &&object) {
    // We need to call the pointed-to object's destructor before freeing the underlying memory.
    std::destroy_at(object.Get());
    Deallocate(object.Get(), sizeof(T));
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
  common::ManagedPointer<MemoryTracker> GetTracker() { return tracker_; }

 private:
  // Metadata tracker for memory allocations
  common::ManagedPointer<MemoryTracker> tracker_;

  // Variable storing the threshold above which to use MMap allocations
  static std::atomic<std::size_t> mmap_threshold;
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
  T *allocate(std::size_t n) { return memory_->AllocateArray<T>(n, false); }  // NOLINT

  /**
   * Deallocates an array
   * @param ptr array to deallocate
   * @param n size of the array
   */
  void deallocate(T *ptr, std::size_t n) { memory_->DeallocateArray(ptr, n); }  // NOLINT

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
 * An STL vector backed by a MemoryPool.
 * @tparam T The element type stored in the vector.
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

}  // namespace noisepage::execution::sql
