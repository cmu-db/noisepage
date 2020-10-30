#pragma once

#include <sys/mman.h>

#include <cerrno>
#include <cstddef>
#include <cstdlib>
#include <cstring>

#include "common/macros.h"
#include "common/managed_pointer.h"
#include "common/math_util.h"
#include "execution/sql/memory_tracker.h"

// Needed for some Darwin machine that don't have MAP_ANONYMOUS
#ifndef MAP_ANONYMOUS
#define MAP_ANONYMOUS MAP_ANON
#endif

namespace noisepage::execution::util {

/**
 * Container for common memory operations.
 */
class Memory {
 public:
  /** This class cannot be instantiated. */
  DISALLOW_INSTANTIATION(Memory);
  /** This class cannot be copied or moved. */
  DISALLOW_COPY_AND_MOVE(Memory);

  /**
   * Allocate and return a pointer to a chunk of memory of @em size bytes. The returned pointer will
   * be a multiple of @em alignment, which must be a power of two and a multiple of sizeof(void *).
   * This function also requires that the requested size be a multiple of the alignment. For this
   * reason, it may over-allocate, but only at most @em (alignment - 1) bytes padded at the end.
   *
   * @param size The number of bytes to allocate.
   * @param alignment The desired alignment.
   * @return A pointer to the allocated chunk of memory. If allocation fails, returns a NULL
   * pointer. It is the caller's responsibility to decide how to handle the error.
   */
  [[nodiscard]] static void *MallocAligned(const std::size_t size, const std::size_t alignment) {
    NOISEPAGE_ASSERT(common::MathUtil::IsPowerOf2(alignment), "Alignment must be a power of two");
    NOISEPAGE_ASSERT(common::MathUtil::IsAligned(alignment, sizeof(void *)),
                     "Alignment must be a multiple of sizeof(void*)");

    void *ptr = nullptr;
#if defined(__APPLE__)
    int32_t ret = posix_memalign(&ptr, alignment, size);
    ptr = (ret == 0 ? ptr : nullptr);
#else
    // STL's aligned allocation requires that the size is a multiple of the alignment as well.
    ptr = std::aligned_alloc(alignment, common::MathUtil::AlignTo(size, alignment));
#endif

    return ptr;
  }

  /**
   * Allocate and return a pointer to a chunk of memory of @em size bytes. The returned memory chunk
   * is backed by huge pages (typically 2MB) that is more TLB-friendly when performing random access
   * within the range.
   *
   * The allocation does not actually allocate physical memory, but reserves VM address space.
   * Physical memory is demand-paged in upon first access. This can potentially be a
   * performance-issue when accessing newly allocated pages in a hot loop. For this reason, the
   * caller can request that allocated pages be pre-faulted and populated immediately by setting @em
   * populate to true.
   *
   * @param size The number of bytes to allocate using huge pages.
   * @param populate Whether to pre-fault the allocated pages backing the memory chunk.
   * @return A pointer to the allocated chunk of memory.
   */
  [[nodiscard]] static void *MallocHuge(const std::size_t size, const bool populate) {
    // Attempt to memory-map an anonymous and private chunk of memory

    // macOS Catalina does not support MAP_POPULATE.
#ifdef MAP_POPULATE
    const int32_t mmap_flags = MAP_PRIVATE | MAP_ANONYMOUS | (populate ? MAP_POPULATE : 0);
#else
    const int32_t mmap_flags = MAP_PRIVATE | MAP_ANONYMOUS;
#endif
    void *const ptr = mmap(nullptr, size, PROT_READ | PROT_WRITE, mmap_flags, -1, 0);

    // If failed, return null. Let client worry.
    if (ptr == MAP_FAILED) {
      return nullptr;
    }

#if !defined(__APPLE__)
    // All good, advise to use huge pages. madvise() docs state that it may return EAGAIN to signal
    // that we should retry.
    int32_t ret;
    do {
      ret = madvise(ptr, size, MADV_HUGEPAGE);
    } while (ret == -1 && errno == EAGAIN);
    NOISEPAGE_ASSERT(ret == 0, "madvise() returned error!");
#endif

    return ptr;
  }

  /**
   * Allocate an array of @em num_elems of type @em T using huge pages.
   * @tparam T The element type of the array.
   * @param tracker Memory Tracker
   * @param num_elems The number of elements in the array.
   * @param populate Whether to pre-fault the allocated pages backing the memory chunk.
   * @return A pointer tot he allocated array.
   */
  template <typename T>
  [[nodiscard]] static T *TrackMallocHugeArray(common::ManagedPointer<sql::MemoryTracker> tracker,
                                               const std::size_t num_elems, const bool populate) {
    std::size_t size = sizeof(T) * num_elems;
    if (tracker != nullptr) tracker->Increment(size);

    void *ptr = MallocHuge(size, populate);
    return reinterpret_cast<T *>(ptr);
  }

  /**
   * Free a chunk of memory pointed to by @em ptr whose size is @em size bytes.
   * @param ptr A pointer to an allocated chunk of memory.
   * @param size The size of the chunk of memory in bytes.
   */
  static void FreeHuge(void *ptr, const std::size_t size) {
    NOISEPAGE_ASSERT(ptr != nullptr, "Cannot free NULL pointer");
    NOISEPAGE_ASSERT(size != 0, "Cannot free zero-sized memory");
    munmap(ptr, size);
  }

  /**
   * Free an array of elements pointed to by @em arr with @em num_elems elements. This array should
   * have been allocated using MallocHugeArray().
   * @tparam T The type of element in the array.
   * @param tracker Memory Tracker
   * @param arr The array of elements to free.
   * @param num_elems The number of elements in the array.
   */
  template <typename T>
  static void TrackFreeHugeArray(common::ManagedPointer<sql::MemoryTracker> tracker, T arr[],
                                 const std::size_t num_elems) {
    NOISEPAGE_ASSERT(arr != nullptr, "Cannot free NULL pointer");
    NOISEPAGE_ASSERT(num_elems != 0, "Cannot free zero-sized memory");
    std::size_t size = sizeof(T) * num_elems;

    if (tracker != nullptr) tracker->Decrement(size);
    FreeHuge(static_cast<void *>(arr), size);
  }

  /**
   * Prefetch a memory address into CPU cache. Use the template arguments to control whether the
   * prefetch is intended for a subsequent read or write, and the degree of locality for the
   * address.
   *
   * @tparam READ A boolean indicating whether the prefetch request is for memory that will be
   *              read-only or written to.
   * @tparam LOCALITY A hint indicating the temporal locality (i.e., reuse) of the memory address.
   *                  For high-reuse memory, the fetch will attempt to place the memory into L1
   *                  cache. For low-reuse memory, the fetch will attempt to place the memory into
   *                  slower-but-larger L3 cache.
   * @param addr The address to issue a prefetch for.
   */
  template <bool READ, Locality LOCALITY>
  static void Prefetch(const void *const addr) noexcept {
    // The builtin prefetch intrinsic takes three arguments:
    // 'addr': the address we want to prefetch
    // 'rw': indicates read-write intention; 0 is for a read, 1 is for a write
    // 'locality': indicates the degree of temporal locality represented in the
    // range {0-3}. 0 means no locality; 3 is high temporal locality.
    __builtin_prefetch(addr, READ ? 0 : 1, static_cast<uint8_t>(LOCALITY));
  }
};

}  // namespace noisepage::execution::util
