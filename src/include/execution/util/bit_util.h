#pragma once

#include <memory>
#include <utility>

#include "common/constants.h"
#include "common/macros.h"
#include "common/math_util.h"

namespace noisepage::execution::util {

/**
 * Utility class to deal with bit-level operations.
 */
class BitUtil {
 public:
  /** This class cannot be instantiated. */
  DISALLOW_INSTANTIATION(BitUtil);
  /** This class cannot be copied or moved. */
  DISALLOW_COPY_AND_MOVE(BitUtil);

  /** The number of bits in one word. */
  static constexpr const uint32_t K_BIT_WORD_SIZE = sizeof(uint32_t) * common::Constants::K_BITS_PER_BYTE;

  // Make sure the number of bits in a word is a power of two to make all these bit operations cheap.
  static_assert(common::MathUtil::IsPowerOf2(K_BIT_WORD_SIZE));

  /**
   * @return A count of the number of unset (i.e., '0') bits from the most significant bit to the
   *         least, stopping at the first set (i.e., '1') bit in the input integral value @em val.
   */
  template <typename T>
  static constexpr uint64_t CountLeadingZeros(T val) {
    return llvm::countLeadingZeros(val);
  }

  /**
   * @return A count of the number of unset (i.e., '0') bits from the least significant bit to the
   *         most, stopping at the first set (i.e., '1') bit in the input integral value @em val.
   */
  template <typename T>
  static constexpr uint64_t CountTrailingZeros(T val) {
    return llvm::countTrailingZeros(val);
  }

  /**
   * @return A count of the number of set (i.e., '1') bits in the given integral value @em val.
   */
  template <typename T>
  static constexpr uint32_t CountPopulation(T val) {
    return llvm::countPopulation(val);
  }

  /**
   * @return The number of words needed to store a bit vector with @em num_bits bits, rounded up to
   *         the next word size.
   */
  static constexpr uint64_t Num32BitWordsFor(uint64_t num_bits) {
    return noisepage::common::MathUtil::DivRoundUp(num_bits, K_BIT_WORD_SIZE);
  }

  /**
   * Test if the bit at index @em idx is set in the bit vector.
   * @param bits The bit vector.
   * @param idx The index of the bit to check.
   * @return True if set; false otherwise.
   */
  static constexpr bool Test(const uint32_t bits[], const uint32_t idx) {
    const uint32_t mask = 1u << (idx % K_BIT_WORD_SIZE);
    return static_cast<bool>(bits[idx / K_BIT_WORD_SIZE] & mask);
  }

  /**
   * Set the bit at index @em idx to 1 in the bit vector @em bits.
   * @param bits The bit vector.
   * @param idx The index of the bit to set to 1.
   */
  static constexpr void Set(uint32_t bits[], const uint32_t idx) {
    bits[idx / K_BIT_WORD_SIZE] |= 1u << (idx % K_BIT_WORD_SIZE);
  }

  /**
   * Set the bit at index @em idx to the boolean indicated by @em val
   * @param bits The bit vector
   * @param idx The index of the bit to set or unset
   * @param val The value to set the bit to
   */
  static constexpr void SetTo(uint32_t bits[], const uint32_t idx, const bool val) {
    if (val) {
      Set(bits, idx);
    } else {
      Unset(bits, idx);
    }
  }

  /**
   * Set the bit at index @em idx to 0 in the bit vector @em bits.
   * @param bits The bit vector.
   * @param idx The index of the bit to set to 0.
   */
  static constexpr void Unset(uint32_t bits[], const uint32_t idx) {
    bits[idx / K_BIT_WORD_SIZE] &= ~(1u << (idx % K_BIT_WORD_SIZE));
  }

  /**
   * Flip the value of the bit at index @em idx in the bit vector.
   * @param bits The bit vector.
   * @param idx The index of the bit to flip.
   */
  static constexpr void Flip(uint32_t bits[], const uint32_t idx) {
    bits[idx / K_BIT_WORD_SIZE] ^= 1u << (idx % K_BIT_WORD_SIZE);
  }

  /**
   * Clear all the bits in the bit vector, setting them to 0
   * @param bits bit vector to be cleared
   * @param num_bits size of the bit vector, in bits
   */
  static void Clear(uint32_t bits[], const uint64_t num_bits) {
    auto num_words = Num32BitWordsFor(num_bits);
    std::memset(bits, 0, num_words * sizeof(uint32_t));
  }
};

}  // namespace noisepage::execution::util
