#pragma once
#include <cerrno>
#include <cstdlib>

#include "common/strong_typedef.h"
namespace noisepage {
// Use byte for raw byte storage instead of char so string functions are explicitly disabled for those.
using byte = std::byte;

namespace common {
/**
 * Static utility class for more advanced memory allocation behavior
 */
struct AllocationUtil {
  AllocationUtil() = delete;

  /**
   * Allocates a chunk of memory whose start address is guaranteed to be aligned to 8 bytes
   * @param byte_size size of the memory chunk to allocate, in bytes
   * @return allocated memory pointer
   */
  static byte *AllocateAligned(uint64_t byte_size) {
    // This is basically allocating the chunk as a 64-bit array, which forces c++ to give back to us
    // 8 byte-aligned addresses. + 7 / 8 is equivalent to padding up the nearest 8-byte size. We
    // use this hack instead of std::aligned_alloc because calling delete on it does not make ASAN
    // happy on Linux + GCC, and calling std::free on pointers obtained from new is undefined behavior.
    // Having to support two paradigms when we liberally use byte * throughout the codebase is a
    // maintainability nightmare.
    return reinterpret_cast<byte *>(new uint64_t[(byte_size + 7) / 8]);
  }

  /**
   * Allocates an array of elements that start at an 8-byte aligned address
   * @tparam T type of element
   * @param size number of elements to allocate
   * @return allocated memory pointer
   */
  template <class T>
  static T *AllocateAligned(uint32_t size) {
    return reinterpret_cast<T *>(AllocateAligned(size * sizeof(T)));
  }
};

/**
 * Allocator that allocates and destroys a byte array. Memory location returned by this default allocator is
 * not zeroed-out. The address returned is guaranteed to be aligned to 8 bytes.
 * @tparam T object whose size determines the byte array size.
 */
template <typename T>
class ByteAlignedAllocator {
 public:
  /**
   * Allocates a new byte array sized to hold a T.
   * @return a pointer to the byte array allocated.
   */
  T *New() {
    auto *result = reinterpret_cast<T *>(AllocationUtil::AllocateAligned(sizeof(T)));
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
  void Delete(T *const ptr) { delete[] reinterpret_cast<byte *>(ptr); }  // NOLINT
  // clang-tidy believes we are trying to free released memory.
  // We believe otherwise, hence we're telling it to shut up.
};
}  // namespace common
}  // namespace noisepage
