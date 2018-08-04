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
  static void Deallocate(RawConcurrentBitmap *map) { delete[] reinterpret_cast<uint8_t *>(map); }

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
   * @param bitmap_num_bits number of bits in the bitmap.
   * @param start_pos start searching from this bit location.
   * @param[out] out_pos the position of the first unset bit will be written here, if it exists.
   * @return true if an unset bit was found, and false otherwise.
   */
  bool FirstUnsetPos(uint32_t bitmap_num_bits, uint32_t start_pos, uint32_t *out_pos) {
    // invalid starting position.
    if (start_pos >= bitmap_num_bits) {
      return false;
    }

    uint32_t num_bytes = BitmapSize(bitmap_num_bits);  // maximum number of bytes in the bitmap
    uint32_t byte_pos = start_pos / BYTE_SIZE;         // current byte position
    uint32_t search_width = sizeof(uint64_t);          // number of bits searched at a time
    uint32_t bits_left = bitmap_num_bits;              // number of bits remaining
    bool found_unset_bit = false;                      // whether we found an unset bit previously

    while (byte_pos < num_bytes && bits_left > 0) {
      // as soon as we find an unset bit, we go back to looking at single bytes
      if (found_unset_bit || search_width < sizeof(uint16_t)) {
        search_width = sizeof(uint8_t);
      } else if (bits_left >= sizeof(uint64_t) * BYTE_SIZE) {
        search_width = sizeof(uint64_t);
      } else if (bits_left >= sizeof(uint32_t) * BYTE_SIZE) {
        search_width = sizeof(uint32_t);
      } else if (bits_left >= sizeof(uint16_t) * BYTE_SIZE) {
        search_width = sizeof(uint16_t);
      } else {
        search_width = sizeof(uint8_t);
      }

      // try to look for an unset bit.
      switch (search_width) {
        case sizeof(uint64_t):
          found_unset_bit = FindUnsetBit<int64_t>(&byte_pos, &bits_left);
          break;
        case sizeof(uint32_t):
          found_unset_bit = FindUnsetBit<int32_t>(&byte_pos, &bits_left);
          break;
        case sizeof(uint16_t):
          found_unset_bit = FindUnsetBit<int16_t>(&byte_pos, &bits_left);
          break;
        case sizeof(uint8_t): {
          uint8_t bits = bits_[byte_pos].load();
          if (static_cast<std::atomic<int8_t>>(bits) != -1) {
            // we have a byte with an unset bit inside. we return that location, which may be stale.
            // we don't bother ensuring freshness since our function's result is immediately stale.
            for (uint32_t pos = 0; pos < BYTE_SIZE; pos++) {
              // we are always padded to a byte, but we don't want to use the padding.
              if (pos + byte_pos * BYTE_SIZE >= bitmap_num_bits) {
                return false;
              }
              // if we find a free bit, we return that.
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
        default:
          PELOTON_ASSERT(false);
      }
    }

    return false;
  }

  // TODO(Tianyu): We will eventually need optimization for bulk checks and
  // bulk flips. This thing is embarrassingly easy to vectorize.

 private:
  std::atomic<uint8_t> bits_[0];

  /**
   * Looks for an unset bit in a T-sized word from the bitmap, starting at byte_pos.
   * If an unset bit is found, returns true.
   * Otherwise updates byte_pos to be the next place we should search.
   * @tparam T signed fixed width integer type.
   * @param[in,out] byte_pos invariant: next byte position we should search.
   * @param[in,out] bits_left the number of valid bits remaining.
   * @return true if an unset bit was found, false otherwise.
   */
  template <class T>
  bool FindUnsetBit(uint32_t *byte_pos, uint32_t *bits_left) {
    // for a signed integer, -1 represents that all the bits are set
    T bits = reinterpret_cast<std::atomic<T> *>(&bits_[*byte_pos])->load();
    if (bits == static_cast<T>(-1)) {
      *byte_pos += static_cast<uint32_t>(sizeof(T));
      // prevent underflow
      if (*bits_left < sizeof(T) * BYTE_SIZE) {
        *bits_left = 0;
      } else {
        *bits_left = *bits_left - static_cast<uint32_t>(sizeof(T) * BYTE_SIZE);
      }
      return false;
    }
    return true;
  }
};

// WARNING: DO NOT CHANGE THE CLASS LAYOUT OF RawConcurrentBitmap.
// The correctness of our storage code depends in this class having this
// exact layout. Changes include marking a function as virtual, as that adds
// a Vtable to the class layout,
static_assert(sizeof(RawConcurrentBitmap) == 0, "Unexpected RawConcurrentBitmap layout!");
}  // namespace common
}  // namespace terrier
