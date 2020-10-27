#pragma once

#include <llvm/Support/Allocator.h>

#include "execution/util/region.h"

namespace noisepage::execution::util {

/**
 * An STL-compliant allocator that uses a region-based strategy
 * @tparam T The types of elements this allocator handles
 */
template <typename T>
class StlRegionAllocator {
 public:
  /**
   * value_type typedef for the allocator
   */
  using value_type = T;

  /**
   * Constructor
   * @param region region to use for allocation
   */
  explicit StlRegionAllocator(Region *region) noexcept : region_(region) {}

  /**
   * Copy constructor
   * @param other allocator to copy
   */
  StlRegionAllocator(const StlRegionAllocator &other) noexcept : region_(other.region_) {}

  /**
   * Copy constructor
   * @tparam U value_type of the other allocator
   * @param other allocator to copy
   */
  template <typename U>
  explicit StlRegionAllocator(const StlRegionAllocator<U> &other) noexcept : region_(other.region_) {}

  // Make region allocators for other type friends of this one
  template <typename U>
  friend class StlRegionAllocator;

  /**
   * Allocates an array of size n, and of type T*
   * @param n size of the array
   * @return pointer to the array
   */
  T *allocate(std::size_t n) {  // NOLINT
    return region_->AllocateArray<T>(n);
  }

  // No-op
  /**
   * Does nothing. Everything in the region will be de-allocated when the region goes
   * @param ptr pointer to deallocate
   * @param n size of allocated array
   */
  void deallocate(T *ptr, std::size_t n) {}  // NOLINT

  /**
   * Equality comparator
   * @param other other allocator to compare to
   * @return whether this has the same region as the other allocator
   */
  bool operator==(const StlRegionAllocator &other) const { return region_ == other.region_; }

  /**
   * Unequality
   * @param other other allocator to compare to
   * @return whether this has a different region as the other allocator
   */
  bool operator!=(const StlRegionAllocator &other) const { return !(*this == other); }

 private:
  Region *region_;
};

/**
 * An allocator that complies with LLVM's allocator concept and uses a region-
 * based allocation strategy.
 */
class LLVMRegionAllocator : public llvm::AllocatorBase<LLVMRegionAllocator> {
 public:
  /**
   * Constructor
   * @param region region to use for allocation
   */
  explicit LLVMRegionAllocator(Region *region) noexcept : region_(region) {}

  /**
   * Allocates an array of size n
   * @param size of the array to allocate
   * @param alignment alignment to use
   * @return pointer to the array
   */
  void *Allocate(std::size_t size, std::size_t alignment) { return region_->Allocate(size, alignment); }

  /**
   * Pull in base class overloads.
   */
  using AllocatorBase<LLVMRegionAllocator>::Allocate;

  /**
   * Deallocates an array. This is a no-op for regions.
   * @param ptr pointer to deallocate.
   * @param size size of the array.
   */
  void Deallocate(const void *ptr, std::size_t size) { region_->Deallocate(ptr, size); }

  /**
   * Pull in base class overloads.
   */
  using AllocatorBase<LLVMRegionAllocator>::Deallocate;

 private:
  Region *region_;
};

}  // namespace noisepage::execution::util
