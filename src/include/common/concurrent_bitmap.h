#pragma once
#include <memory>
#include "common/container/bitmap.h"
#include "common/typedefs.h"

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
  ~RawConcurrentBitmap() = delete;
  DISALLOW_COPY_AND_MOVE(RawConcurrentBitmap)

  /**
   * Allocates a new RawConcurrentBitmap of size num_bits. Up to the caller to call
   * Deallocate on its return value
   * @param num_bits number of bits in the bitmap
   * @return ptr to new RawConcurrentBitmap
   */
  static RawConcurrentBitmap *Allocate(uint32_t num_bits) {
    uint32_t num_bytes = BitmapSize(num_bits);
    auto *result = new uint8_t[num_bytes];
    PELOTON_MEMSET(result, 0, num_bytes);
    return reinterpret_cast<RawConcurrentBitmap *>(result);
  }

  /**
   * Deallocates a RawConcurrentBitmap. Only call on pointers given out by Allocate
   * @param map the map to deallocate
   */
  static void Deallocate(RawConcurrentBitmap *map) { delete reinterpret_cast<uint8_t *>(map); }

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

  /**
   * Returns the position of the first unset bit, if it exists.
   * Note that this result is immediately stale.
   * @param num_bits number of bits in the bitmap.
   * @param start_pos start searching from this bit location.
   * @param[out] out_pos the position of the first unset bit will be written here, if it exists.
   * @return true if an unset bit was found, and false otherwise.
   */
  bool FirstUnsetPos(uint32_t num_bits, uint32_t start_pos, uint32_t *out_pos) {
    uint32_t num_bytes = BitmapSize(num_bits);  // maximum number of bytes in the bitmap
    uint32_t byte_pos = start_pos / BYTE_SIZE;  // current byte position
    uint32_t search_width;                      // number of bits searched at a time

    // optimization: don't search wider than the number of bits we have
    if (num_bits <= 8) {
      search_width = 8;
    } else if (num_bits <= 16) {
      search_width = 16;
    } else if (num_bits <= 32) {
      search_width = 32;
    } else {
      search_width = 64;
    }

    while (byte_pos < num_bytes) {
      // each case loads a word of search_width bits and casts it as a signed type.
      // if all bits are set (equals -1), then we can refine our search.
      switch (search_width) {
        default:
        case 64:
          if (static_cast<std::atomic<int64_t>>(bits_[byte_pos]).load() != -1) {
            search_width = 32;
          } else {
            byte_pos += 8;
          }
          break;
        case 32:
          if (static_cast<std::atomic<int32_t>>(bits_[byte_pos]).load() != -1) {
            search_width = 16;
          } else {
            byte_pos += 4;
          }
          break;
        case 16:
          if (static_cast<std::atomic<int16_t>>(bits_[byte_pos]).load() != -1) {
            search_width = 8;
          } else {
            byte_pos += 2;
          }
          break;
        case 8:
          uint8_t bits = bits_[byte_pos].load();
          if (static_cast<std::atomic<int8_t>>(bits) != -1) {
            // we have a byte with an unset bit inside. we return that location, which may be stale.
            // we don't bother ensuring freshness since our function's result is immediately stale.
            for (uint32_t pos = 0; pos < BYTE_SIZE && pos + byte_pos * BYTE_SIZE < num_bits; pos++) {
              bool is_set = static_cast<bool>(bits & ONE_HOT_MASK(pos));
              if (!is_set) {
                *out_pos = pos + byte_pos * BYTE_SIZE;
                return true;
              }
            }
          } else {
            byte_pos += 1;
          }
          break;
      }
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
// exact layout. Changes include marking a function as virtual, as that adds
// a Vtable to the class layout,
static_assert(sizeof(RawConcurrentBitmap) == 0, "Unexpected RawConcurrentBitmap layout!");
}  // namespace common
}  // namespace terrier
