#pragma once

#include <cstdint>
#include <limits>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>

#include "common/macros.h"
#include "common/math_util.h"
#include "execution/util/execution_common.h"

namespace noisepage::execution::util {

/**
 * A region-based allocator supports fast O(1) time allocations of small chunks of memory.
 * Individual de-allocations are not supported, but the entire region can be de-allocated in one
 * fast operation upon destruction. Regions are used to hold ephemeral objects that are allocated
 * once and freed all at once. This is the pattern used during parsing when generating AST nodes
 * which are thrown away after compilation to bytecode.
 */
class Region {
 public:
  /**
   * Construct a region with the given name @em name. No allocations are performed upon construction, only at the first
   * call to @em Allocate().
   */
  explicit Region(std::string name) noexcept;

  /**
   * Default move constructor.
   */
  Region(Region &&that) noexcept
      : name_(std::move(that.name_)),
        allocated_(that.allocated_),
        alignment_waste_(that.alignment_waste_),
        chunk_bytes_allocated_(that.chunk_bytes_allocated_),
        head_(that.head_),
        position_(that.position_),
        end_(that.end_) {
    that.name_.clear();
    that.allocated_ = 0;
    that.alignment_waste_ = 0;
    that.chunk_bytes_allocated_ = 0;
    that.head_ = nullptr;
    that.position_ = 0;
    that.end_ = 0;
  }

  /**
   * Regions cannot be copied.
   */
  DISALLOW_COPY(Region);

  /**
   * Destructor. All allocated memory is freed here.
   */
  ~Region();

  /**
   * Default move assignment.
   */
  Region &operator=(Region &&that) noexcept {
    std::swap(name_, that.name_);
    std::swap(allocated_, that.allocated_);
    std::swap(alignment_waste_, that.alignment_waste_);
    std::swap(chunk_bytes_allocated_, that.chunk_bytes_allocated_);
    std::swap(head_, that.head_);
    std::swap(position_, that.position_);
    std::swap(end_, that.end_);
    return *this;
  }

  /**
   * Allocate memory from this region
   * @param size The number of bytes to allocate
   * @param alignment The desired alignment
   * @return A pointer to the start of the allocated space
   */
  void *Allocate(std::size_t size, std::size_t alignment = DEFAULT_BYTE_ALIGNMENT);

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
   * Individual de-allocations in a region-allocator are a no-op. All memory is freed when the
   * region is destroyed, or manually through a call to Region::FreeAll().
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
   * @return The name of the region.
   */
  const std::string &Name() const { return name_; }

  /**
   * @return The number of bytes this region has given out.
   */
  uint64_t Allocated() const { return allocated_; }

  /**
   * @return The number of bytes wasted due to alignment requirements.
   */
  uint64_t AlignmentWaste() const { return alignment_waste_; }

  /**
   * @return The total number of bytes acquired from the OS.
   */
  uint64_t TotalMemory() const { return chunk_bytes_allocated_; }

 private:
  // Expand the region
  uintptr_t Expand(std::size_t requested);

 private:
  /**
   * A chunk represents a physically contiguous "chunk" of memory. It is the smallest unit of allocation a region
   * acquires from the operating system. Each individual region allocation is sourced from a chunk. */
  struct Chunk {
    Chunk *next_;
    uint64_t size_;

    void Init(Chunk *next, uint64_t size) {
      this->next_ = next;
      this->size_ = size;
    }

    uintptr_t Start() const { return reinterpret_cast<uintptr_t>(this) + sizeof(Chunk); }

    uintptr_t End() const { return reinterpret_cast<uintptr_t>(this) + size_; }
  };

 private:
  /** The alignment of all pointers. */
  static const uint32_t DEFAULT_BYTE_ALIGNMENT = 8;

  /** Min chunk allocation is 8KB. */
  static const std::size_t MIN_CHUNK_ALLOCATION = 8 * 1024;

  /** Max chunk allocation is 1MB. */
  static const std::size_t MAX_CHUNK_ALLOCATION = 1 * 1024 * 1024;

  /** The name of the region. */
  std::string name_;

  /** The number of bytes allocated by this region. */
  std::size_t allocated_;

  /** Bytes wasted due to alignment. */
  std::size_t alignment_waste_;

  /** The total number of chunk bytes. This may include bytes not yet given out by the region. */
  std::size_t chunk_bytes_allocated_;

  /** The head of the chunk list. */
  Chunk *head_;

  /** The position in the current free chunk where the next allocation can happen. */
  uintptr_t position_;
  /** Together with position_, makes up the contiguous space [position, limit) where allocations can happen from. */
  uintptr_t end_;
};

/**
 * Base class for objects allocated from a region
 */
class RegionObject {
 public:
  // Region objects should always be allocated from and released from a region.
  /** Should not be called. */
  void *operator new(std::size_t size) = delete;
  /** Should not be called. */
  void operator delete(void *ptr) = delete;

  /**
   * Allocation.
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
  void operator delete(UNUSED_ATTRIBUTE void *ptr, UNUSED_ATTRIBUTE Region *region) {
    UNREACHABLE("Calling \"delete\" on region object is forbidden!");
  }
};

}  // namespace noisepage::execution::util
