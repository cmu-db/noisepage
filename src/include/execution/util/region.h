#pragma once

#include <cstdint>
#include <limits>
#include <string>
#include <type_traits>

#include "execution/util/common.h"
#include "execution/util/macros.h"
#include "execution/util/math_util.h"

namespace terrier::util {

/**
 * A region-based allocator supports fast O(1) time allocations of small chunks
 * of memory. Individual de-allocations are not supported, but the entire
 * region can be de-allocated in one fast operation upon destruction. Regions
 * are used to hold ephemeral objects that are allocated once and freed all at
 * once. This is the pattern used during parsing when generating AST nodes
 * which are thrown away after compilation to bytecode.
 */
class Region {
 public:
  /**
   * Construct a region with the given name @em name. No allocations are
   * performed upon construction, only at the first call to @em Allocate().
   */
  explicit Region(std::string name) noexcept;

  /**
   * Regions cannot be copied or moved
   */
  DISALLOW_COPY_AND_MOVE(Region);

  /**
   * Destructor. All allocated memory is freed here.
   */
  ~Region();

  /**
   * Allocate memory from this region
   * @param size The number of bytes to allocate
   * @param alignment The desired alignment
   * @return A pointer to the start of the allocated space
   */
  void *Allocate(std::size_t size, std::size_t alignment = kDefaultByteAlignment);

  /**
   * Allocate a (contiguous) array of elements of the given type
   * @tparam T The type of each element in the array
   * @param num_elems The number of requested elements in the array
   * @return A pointer to the allocated array
   */
  template <typename T>
  T *AllocateArray(std::size_t num_elems) {
    return static_cast<T *>(Allocate(num_elems * sizeof(T), alignof(T)));
  }

  /**
   * Individual de-allocations in a region-allocator are a no-op. All memory is
   * freed when the region is destroyed, or manually through a call to
   * @em FreeAll().
   * @param ptr The pointer to the memory we're de-allocating
   * @param size The number of bytes the pointer points to
   */
  void Deallocate(const void *ptr, std::size_t size) const {
    // No-op
  }

  /**
   * Free all allocated objects in one fell swoop
   */
  void FreeAll();

  // -------------------------------------------------------
  // Accessors
  // -------------------------------------------------------

  /**
   * @return The name of the region
   */
  const std::string &name() const { return name_; }

  /**
   * @return The number of bytes this region has given out
   */
  u64 allocated() const { return allocated_; }

  /**
   * @return The number of bytes wasted due to alignment requirements
   */
  u64 alignment_waste() const { return alignment_waste_; }

  /**
   * @return The total number of bytes acquired from the OS
   */
  u64 total_memory() const { return chunk_bytes_allocated_; }

 private:
  // Expand the region
  uintptr_t Expand(std::size_t requested);

 private:
  // A chunk represents a physically contiguous "chunk" of memory. It is the
  // smallest unit of allocation a region acquires from the operating system.
  // Each individual region allocation is sourced from a chunk.
  struct Chunk {
    Chunk *next;
    u64 size;

    void Init(Chunk *next, u64 size) {
      this->next = next;
      this->size = size;
    }

    uintptr_t Start() const { return reinterpret_cast<uintptr_t>(this) + sizeof(Chunk); }

    uintptr_t End() const { return reinterpret_cast<uintptr_t>(this) + size; }
  };

 private:
  // The alignment of all pointers
  static const u32 kDefaultByteAlignment = 8;

  // Min chunk allocation is 8KB
  static const std::size_t kMinChunkAllocation = 8 * 1024;

  // Max chunk allocation is 1MB
  static const std::size_t kMaxChunkAllocation = 1 * 1024 * 1024;

  // The name of the region
  const std::string name_;

  // The number of bytes allocated by this region
  std::size_t allocated_;

  // Bytes wasted due to alignment
  std::size_t alignment_waste_;

  // The total number of chunk bytes. This may include bytes not yet given out
  // by the region
  std::size_t chunk_bytes_allocated_;

  // The head of the chunk list
  Chunk *head_;

  // The position in the current free chunk where the next allocation can happen
  // These two fields make up the contiguous space [position, limit) where
  // allocations can happen from
  uintptr_t position_;
  uintptr_t end_;
};

/**
 * Base class for objects allocated from a region
 */
class RegionObject {
 public:
  /**
   * Should not be called
   */
  void *operator new(std::size_t size) = delete;

  /**
   * Should not be called
   */
  void operator delete(void *ptr) = delete;

  /**
   * Allocation
   * @param size size of buffer to allocate
   * @param region region to use for allocation
   * @return pointer to allocated buffer
   */
  void *operator new(std::size_t size, Region *region) { return region->Allocate(size); }

  // Objects from a Region shouldn't be deleted individually. They'll be deleted
  // when the region is destroyed. You can invoke this behavior manually by
  // calling Region::FreeAll().
  /**
   * Should not be called.
   */
  void operator delete(UNUSED void *ptr, UNUSED Region *region) {
    UNREACHABLE("Calling \"delete\" on region object is forbidden!");
  }
};

}  // namespace terrier::util
