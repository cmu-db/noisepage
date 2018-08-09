#pragma once

#include <memory>
#include "common/typedefs.h"

#ifndef BYTE_SIZE
#define BYTE_SIZE 8u
#endif

// Some platforms would have already defined the macro. But its presence is
// not standard and thus not portable. Pretty sure this is always 8 bits.
// If not, consider getting a new machine, and preferably not from another
// dimension :)
static_assert(BYTE_SIZE == 8u, "BYTE_SIZE should be set to 8!");

// n must be [0, 7], all 0 except for 1 on the nth bit
#define ONE_HOT_MASK(n) (1u << (BYTE_SIZE - (n)-1u))
// n must be [0, 7], all 1 except for 0 on the nth bit
#define ONE_COLD_MASK(n) (0xFF - ONE_HOT_MASK(n))

namespace terrier::common {
constexpr uint32_t BitmapSize(uint32_t n) { return n % BYTE_SIZE == 0 ? n / BYTE_SIZE : n / BYTE_SIZE + 1; }

/**
 * A RawBitmap is a bitmap that does not have the compile-time
 * information about sizes, because we expect it to be reinterpreted from
 * raw memory bytes.
 *
 * Therefore, you should never construct an instance of a RawBitmap.
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
class RawBitmap {
 public:
  // Always reinterpret_cast from raw memory.
  RawBitmap() = delete;
  ~RawBitmap() = delete;
  DISALLOW_COPY_AND_MOVE(RawBitmap)

  /**
   * Allocates a new RawBitmap of size num_bits.
   * Up to the caller to call Deallocate on its return value.
   * @param num_bits number of bits in the bitmap.
   * @return ptr to new RawBitmap.
   */
  static RawBitmap *Allocate(uint32_t num_bits) {
    auto size = BitmapSize(num_bits);
    auto *result = new uint8_t[size];
    PELOTON_MEMSET(result, 0, size);
    return reinterpret_cast<RawBitmap *>(result);
  }

  /**
   * Deallocates a RawBitmap. Only call on pointers given out by Allocate
   * @param map the map to deallocate
   */
  static void Deallocate(RawBitmap *map) { delete[] reinterpret_cast<uint8_t *>(map); }

  /**
   * Test the bit value at the given position
   * @param pos position to test
   * @return true if 1, false if 0
   */
  bool Test(uint32_t pos) const { return static_cast<bool>(bits_[pos / BYTE_SIZE] & ONE_HOT_MASK(pos % BYTE_SIZE)); }

  /**
   * Test the bit value at the given position
   * @param pos position to test
   * @return true if 1, false if 0
   */
  bool operator[](uint32_t pos) const { return Test(pos); }

  /**
   * Sets the bit value at position to be true.
   * @param pos position to test
   * @param val value to set to
   * @return self-reference for chaining
   */
  RawBitmap &Set(uint32_t pos, bool val) {
    if (val)
      bits_[pos / BYTE_SIZE] |= static_cast<uint8_t>(ONE_HOT_MASK(pos % BYTE_SIZE));
    else
      bits_[pos / BYTE_SIZE] &= static_cast<uint8_t>(ONE_COLD_MASK(pos % BYTE_SIZE));
    return *this;
  }

  /**
   * @brief Flip the bit
   * @param pos the position of the bit to flip
   * @return self-reference for chaining
   */
  RawBitmap &Flip(uint32_t pos) {
    bits_[pos / BYTE_SIZE] ^= static_cast<uint8_t>(ONE_HOT_MASK(pos % BYTE_SIZE));
    return *this;
  }

  /**
   * Clears the bitmap by setting bits to 0.
   * @param num_bits number of bits to clear.
   */
  void Clear(uint32_t num_bits) {
    auto size = BitmapSize(num_bits);
    PELOTON_MEMSET(bits_, 0, size);
  }

 private:
  uint8_t bits_[0];
};

// WARNING: DO NOT CHANGE THE CLASS LAYOUT OF RawBitmap.
// The correctness of our storage code depends in this class having this
// exact layout. Changes include marking a function as virtual, as that adds a
// Vtable to the class layout,
static_assert(sizeof(RawBitmap) == 0, "Unexpected RawBitmap layout!");
}  // namespace terrier::common
