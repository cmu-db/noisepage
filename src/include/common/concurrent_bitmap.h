#pragma once
#include <memory>
#include "common/common_defs.h"
#include "common/container/bitmap.h"

// This code should not compile if these assumptions are not true.
static_assert(sizeof(std::atomic<uint8_t>) == sizeof(uint8_t), "unexpected std::atomic size for 8-bit ints");
static_assert(sizeof(std::atomic<uint64_t>) == sizeof(uint64_t), "unexpected std::atomic size for 64-bit ints");

namespace terrier {
namespace common {

/**
 * A RawConcurrentBitmap is a bitmap that does not have the compile-time
 * information about sizes, because we expect it to be reinterpreted from
 * raw memory bytes.
 *
 * Therefore, you should never construct an instance of a RawConcurrentBitmap.
 * Reinterpret an existing block of memory that you know will be a valid bitmap.
 *
 * Use @see terrier::BitmapSize to get the correct size for a bitmap of n
 * elements. Beware that because the size information is lost at compile time,
 * there is ABSOLUTELY no bounds check and you have to rely on programming
 * discipline to ensure safe access.
 *
 * For easy initialization in tests and such, use the static Allocate and
 * Deallocate methods
 */
class RawConcurrentBitmap {
 public:
  // Always reinterpret_cast from raw memory.
  RawConcurrentBitmap() = delete;
  DISALLOW_COPY_AND_MOVE(RawConcurrentBitmap);
  ~RawConcurrentBitmap() = delete;

  /**
   * Allocates a new RawConcurrentBitmap of size. Up to the caller to call
   * Deallocate on its return value
   * @param size number of bits in the bitmap
   * @return ptr to new RawConcurrentBitmap
   */
  static RawConcurrentBitmap *Allocate(uint32_t size) {
    uint32_t num_bytes = BitmapSize(size);
    auto *result = new uint8_t[num_bytes];
    PELOTON_MEMSET(result, 0, num_bytes);
    return reinterpret_cast<RawConcurrentBitmap *>(result);
  }

  /**
   * Deallocates a RawConcurrentBitmap. Only call on pointers given out by Allocate
   * @param map the map to deallocate
   */
  static void Deallocate(RawConcurrentBitmap *map) { delete (uint8_t *)map; }

  /**
   * Test the bit value at the given position
   * @param pos position to test
   * @return true if 1, false if 0
   */
  bool Test(uint32_t pos) const {
    return static_cast<bool>(bits_[pos / BYTE_SIZE].load() & ONE_HOT_MASK(pos % BYTE_SIZE));
  }

  /**
   * Test the bit value at the given position
   * @param pos position to test
   * @return true if 1, false if 0
   */
  bool operator[](uint32_t pos) const { return Test(pos); }

  /**
   * @brief Flip the bit only if current value is actually expected_val
   * The expected_val is needed to guard against the following situation:
   * Caller 1 flips from 0 to 1, Caller 2 flips from 0 to 1, without using
   * expected_val, both calls could succeed with the resulting bit value of 0
   * @param pos the position of the bit to flip
   * @param expected_val the expected current value of the bit to be flipped
   * @return true if flip succeeds, otherwise expected_val didn't match
   */
  bool Flip(uint32_t pos, bool expected_val) {
    uint32_t element = pos / BYTE_SIZE;
    auto mask = static_cast<uint8_t>(ONE_HOT_MASK(pos % BYTE_SIZE));
    for (uint8_t old_val = bits_[element]; static_cast<bool>(old_val & mask) == expected_val;
         old_val = bits_[element]) {
      uint8_t new_val = old_val ^ mask;
      if (bits_[element].compare_exchange_strong(old_val, new_val)) return true;
    }
    return false;
  }

  // TODO(Tianyu): We will eventually need optimization for bulk checks and
  // bulk flips. This thing is embarrassingly easy to vectorize.

 private:
  std::atomic<uint8_t> bits_[0];
};

// WARNING: DO NOT CHANGE THE CLASS LAYOUT OF RawConcurrentBitmap.
// The correctness of our storage code depends in this class having this
// exact layout. Changes include marking a function as virtual (or use the
// FAKED_IN_TESTS macro), as that adds a Vtable to the class layout,
static_assert(sizeof(RawConcurrentBitmap) == 0, "Unexpected RawConcurrentBitmap layout!");
}  // namespace common
}  // namespace terrier
