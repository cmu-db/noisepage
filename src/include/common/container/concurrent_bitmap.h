#pragma once

#include <cstring>
#include <memory>

#include "common/allocator.h"
#include "common/container/bitmap.h"
#include "common/strong_typedef.h"

// This code should not compile if these assumptions are not true.
static_assert(sizeof(std::atomic<uint8_t>) == sizeof(uint8_t), "unexpected std::atomic size for 8-bit ints");
static_assert(sizeof(std::atomic<uint64_t>) == sizeof(uint64_t), "unexpected std::atomic size for 64-bit ints");

namespace terrier::common {

/**
 * A RawConcurrentBitmap is a bitmap that does not have the compile-time
 * information about sizes, because we expect it to be reinterpreted from
 * raw memory bytes.
 *
 * Therefore, you should never construct an instance of a RawConcurrentBitmap.
 * Reinterpret an existing block of memory that you know will be a valid bitmap.
 *
 * Use @see common::BitmapSize to get the correct size for a bitmap of n
 * elements. Beware that because the size information is lost at compile time,
 * there is ABSOLUTELY no bounds check and you have to rely on programming
 * discipline to ensure safe access.
 *
 * For easy initialization in tests and such, use the static Allocate and
 * Deallocate methods.
 *
 * We require RawConcurrentBitmap to be always aligned to 64-bits on byte 0.
 */
class RawConcurrentBitmap {
 public:
  MEM_REINTERPRETATION_ONLY(RawConcurrentBitmap)

  /**
   * Allocates a new RawConcurrentBitmap of size num_bits. Up to the caller to call
   * Deallocate on its return value
   * @param num_bits number of bits (elements to represent) in the bitmap
   * @return ptr to new RawConcurrentBitmap
   */
  static RawConcurrentBitmap *Allocate(const uint32_t num_bits) {
    uint32_t num_bytes = RawBitmap::SizeInBytes(num_bits);
    auto *result = AllocationUtil::AllocateAligned(num_bytes);
    TERRIER_ASSERT(reinterpret_cast<uintptr_t>(result) % sizeof(uint64_t) == 0, "Allocate should be 64-bit aligned.");
    std::memset(result, 0, num_bytes);
    return reinterpret_cast<RawConcurrentBitmap *>(result);
  }

  /**
   * Deallocates a RawConcurrentBitmap. Only call on pointers given out by Allocate
   * @param map the map to deallocate
   */
  static void Deallocate(RawConcurrentBitmap *const map) { delete[] reinterpret_cast<uint8_t *>(map); }

  /**
   * Test the bit value at the given position
   * @param pos position to test
   * @return true if 1, false if 0
   */
  bool Test(const uint32_t pos) const {
    return static_cast<bool>(bits_[pos / BYTE_SIZE].load() & LSB_ONE_HOT_MASK(pos % BYTE_SIZE));
  }

  /**
   * Test the bit value at the given position
   * @param pos position to test
   * @return true if 1, false if 0
   */
  bool operator[](const uint32_t pos) const { return Test(pos); }

  /**
   * @brief Flip the bit only if current value is actually expected_val
   * The expected_val is needed to guard against the following situation:
   * Caller 1 flips from 0 to 1, Caller 2 flips from 0 to 1, without using
   * expected_val, both calls could succeed with the resulting bit value of 0
   * @param pos the position of the bit to flip
   * @param expected_val the expected current value of the bit to be flipped
   * @return true if flip succeeds, otherwise expected_val didn't match
   */
  bool Flip(const uint32_t pos, const bool expected_val) {
    const uint32_t element = pos / BYTE_SIZE;
    auto mask = static_cast<uint8_t>(LSB_ONE_HOT_MASK(pos % BYTE_SIZE));
    for (uint8_t old_val = bits_[element]; static_cast<bool>(old_val & mask) == expected_val;
         old_val = bits_[element]) {
      uint8_t new_val = old_val ^ mask;
      if (bits_[element].compare_exchange_strong(old_val, new_val)) return true;
    }
    return false;
  }

  /**
   * Returns the position of the first unset bit, if it exists.
   * We search beginning from start_pos. It does not wrap back if it runs out of bits.
   * Note that this result is immediately stale.
   * Furthermore, this function assumes byte 0 is aligned to 64 bits.
   * @param bitmap_num_bits number of bits in the bitmap.
   * @param start_pos start searching from this bit location.
   * @param[out] out_pos the position of the first unset bit will be written here, if it exists.
   * @return true if an unset bit was found, and false otherwise.
   */
  bool FirstUnsetPos(const uint32_t bitmap_num_bits, const uint32_t start_pos, uint32_t *const out_pos) const {
    // invalid starting position.
    if (start_pos >= bitmap_num_bits) {
      return false;
    }

    TERRIER_ASSERT(reinterpret_cast<uintptr_t>(bits_) % sizeof(uint64_t) == 0, "bits_ should be 64-bit aligned.");

    const uint32_t num_bytes = RawBitmap::SizeInBytes(bitmap_num_bits);  // maximum number of bytes in the bitmap
    uint32_t byte_pos = start_pos / BYTE_SIZE;                           // current byte position
    uint32_t bits_left = bitmap_num_bits - start_pos;                    // number of bits remaining
    bool found_unset_bit = false;                                        // whether we found an unset bit previously

    while (byte_pos < num_bytes && bits_left > 0) {
      // if we haven't found an unset bit yet, we make a wide search.
      // we also ensure that we are aligned to the word boundaries,
      // under the assumption that byte 0 is aligned to 64 bits.
      if (!found_unset_bit && IsAlignedAndFits<uint64_t>(bits_left, byte_pos)) {
        found_unset_bit = FindUnsetBit<int64_t>(&byte_pos, &bits_left);
      } else if (!found_unset_bit && IsAlignedAndFits<uint32_t>(bits_left, byte_pos)) {
        found_unset_bit = FindUnsetBit<int32_t>(&byte_pos, &bits_left);
      } else if (!found_unset_bit && IsAlignedAndFits<uint16_t>(bits_left, byte_pos)) {
        found_unset_bit = FindUnsetBit<int16_t>(&byte_pos, &bits_left);
      } else {
        // otherwise, we will search a byte at a time
        uint8_t bits = bits_[byte_pos].load();
        if (static_cast<std::atomic<int8_t>>(bits) != -1) {
          // we have a byte with at least one unset bit inside.
          // if the bit is valid, we return the bit. however, the bit could be invalid:
          // 1. it could be part of our end padding, 2. it could be before start_pos
          // in which case we must continue to search the next byte, if it exists.
          // we don't bother ensuring result freshness since our returned position is immediately stale.
          for (uint32_t pos = 0; pos < BYTE_SIZE; pos++) {
            uint32_t current_pos = pos + byte_pos * BYTE_SIZE;
            // we are always padded to a byte, but we don't want to use the padding.
            if (current_pos >= bitmap_num_bits) return false;
            // we want to make sure it is after the start pos.
            if (current_pos < start_pos) {
              continue;
            }
            // if we're here, we have a valid position.
            // if it locates an unset bit, return it.
            auto is_set = static_cast<bool>(bits & LSB_ONE_HOT_MASK(pos));
            if (!is_set) {
              *out_pos = current_pos;
              return true;
            }
          }
        }
        // if we didn't return, we somehow didn't get a valid bit
        // e.g. the only free bit available was before start_pos
        // so we always want to increment our byte_pos to ensure progress
        byte_pos += 1;
        // Also decrease bits_left, making sure that start_pos is taken into account
        bits_left -= BYTE_SIZE - (start_pos % BYTE_SIZE);
      }
    }
    return false;
  }

  /**
   * Clears the bitmap by setting bits to 0.
   * @param num_bits number of bits to clear. This should be equal to the number of elements of the entire bitmap or
   * unintended elements may be cleared
   * @warning this is not thread safe!
   */
  void UnsafeClear(const uint32_t num_bits) {
    auto size = RawBitmap::SizeInBytes(num_bits);
    std::memset(bits_, 0, size);
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
  bool FindUnsetBit(uint32_t *const byte_pos, uint32_t *const bits_left) const {
    TERRIER_ASSERT(*bits_left >= sizeof(T) * BYTE_SIZE, "Need to check that there are enough bits left before calling");
    // for a signed integer, -1 represents that all the bits are set
    T bits = reinterpret_cast<const std::atomic<T> *>(&bits_[*byte_pos])->load();
    if (bits == static_cast<T>(-1)) {
      *byte_pos += static_cast<uint32_t>(sizeof(T));
      *bits_left = *bits_left - static_cast<uint32_t>(sizeof(T) * BYTE_SIZE);
      return false;
    }
    return true;
  }

  /**
   * Returns true if a word of size T would both fit and be aligned for the given parameters.
   * This assumes that byte_pos is aligned for 64 bits at 0.
   * @tparam T signed fixed width integer type.
   * @param bits_left the number of valid bits remaining.
   * @param byte_pos the current byte position.
   * @return true if a word of size T would both fit and be aligned.
   */
  template <class T>
  bool IsAlignedAndFits(const uint32_t bits_left, const uint32_t byte_pos) const {
    return bits_left >= sizeof(T) * BYTE_SIZE && byte_pos % sizeof(T) == 0;
  }
};

// WARNING: DO NOT CHANGE THE CLASS LAYOUT OF RawConcurrentBitmap.
// The correctness of our storage code depends in this class having this
// exact layout. Changes include marking a function as virtual, as that adds
// a Vtable to the class layout,
static_assert(sizeof(RawConcurrentBitmap) == 0, "Unexpected RawConcurrentBitmap layout!");
}  // namespace terrier::common
