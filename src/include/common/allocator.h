#pragma once
#include <cstdlib>
#include "common/typedefs.h"
namespace terrier::common {

/**
 * Static utility class for more advanced memory allocation behavior
 */
struct AllocationUtil {
  AllocationUtil() = delete;

  /**
   * Allocates a chunk of memory whose start address is aligned to the given word size (which is
   * 8 bytes by default) If you ever use an argument that is not 8 bytes, you should really know
   * what you are doing.
   * @param byte_size size of the memory chunk to allocate, in bytes
   * @param alignment the word size to align up to, 8 bytes by default
   * @return allocated memory pointer
   */
  static byte *AllocateAligned(uint64_t byte_size, uint64_t alignment = sizeof(uint64_t)) {
    void *result = nullptr;
    posix_memalign(&result, alignment, byte_size);
    return reinterpret_cast<byte *>(result);
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
  void Delete(T *const ptr) { delete[] ptr; }  // NOLINT
  // clang-tidy believes we are trying to free released memory.
  // We believe otherwise, hence we're telling it to shut up.
};
}  // namespace terrier::common
