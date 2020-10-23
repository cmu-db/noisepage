#pragma once

#include <llvm/Support/MathExtras.h>

#include <cstdint>
#include <cstdlib>
#include <numeric>

#include "common/macros.h"

namespace noisepage::common {

/**
 * Utility class containing various math/arithmetic functions
 */
class MathUtil {
 public:
  /** This class cannot be instantiated. */
  DISALLOW_INSTANTIATION(MathUtil);
  /** This class cannot be copied or moved. */
  DISALLOW_COPY_AND_MOVE(MathUtil);

  /**
   * Perform a division taking the ceil of the result
   * @param numerator The numerator
   * @param denominator The denominator
   * @return The result of the division rounded up to the next integer value
   */
  static constexpr uint64_t DivRoundUp(uint64_t numerator, uint64_t denominator) {
    return (numerator + denominator - 1) / denominator;
  }

  /**
   * @return True if the input value is a power of two > 0; false otherwise.
   */
  static constexpr bool IsPowerOf2(uint64_t val) { return llvm::isPowerOf2_64(val); }

  /**
   * @return The next power of two strictly greater than the input value.
   */
  static uint64_t NextPowerOf2(uint64_t val) { return llvm::NextPowerOf2(val); }

  /**
   * @return The next power of two greater than or equal to the input value.
   */
  static uint64_t PowerOf2Ceil(uint64_t val) { return llvm::PowerOf2Ceil(val); }

  /**
   * @return The last power of two less than or equal to the provided input value.
   */
  static uint64_t PowerOf2Floor(uint64_t val) { return llvm::PowerOf2Floor(val); }

  /**
   * Returns whether @em value is aligned to @em alignment. The desired
   * alignment is required to be a power of two.
   *
   * Examples:
   * @code
   * IsAligned(4, 4) = true
   * IsAligned(4, 8) = false
   * IsAligned(16, 8) = true
   * IsAligned(5, 8) = false
   * @endcode
   *
   * @param value The value whose alignment we'll check
   * @param alignment The desired alignment
   * @return Whether the value has the desired alignment
   */
  static constexpr bool IsAligned(uint64_t value, uint64_t alignment) {
    NOISEPAGE_ASSERT(alignment != 0u && IsPowerOf2(alignment), "Align must be a non-zero power of two.");
    return (value & (alignment - 1)) == 0;
  }

  /**
   * A generic version of alignment checking where @em alignment can be any
   * positive integer.
   *
   * Examples:
   * @code
   * IsAligned(5, 5) = true
   * IsAligned(21, 7) = true
   * IsAligned(24, 5) = false;
   * @endcode
   *
   * @param value
   * @param alignment
   * @return
   */
  static constexpr bool IsAlignedGeneric(uint64_t value, uint64_t alignment) {
    NOISEPAGE_ASSERT(alignment != 0u, "Align must be non-zero.");
    return (value % alignment) == 0;
  }

  /**
   * Returns the next integer greater than the provided input value that is a
   * multiple of the given alignment. Eg:
   *
   * Examples:
   * @code
   * AlignTo(5, 8) = 8
   * AlignTo(8, 8) = 8
   * AlignTo(9, 8) = 16
   * @endcode
   *
   * @param value The input value to align
   * @param align The number to align to
   * @return The next value greater than the input value that has the desired
   * alignment.
   */
  static uint64_t AlignTo(uint64_t value, uint64_t align) { return llvm::alignTo(value, align); }

  /**
   * Align @em addr to the given alignment @em alignment
   * @param addr The address fo align
   * @param alignment The desired alignment
   * @return The input address aligned to the desired alignment
   */
  static constexpr uintptr_t AlignAddress(uintptr_t addr, std::size_t alignment) {
    NOISEPAGE_ASSERT(alignment > 0 && MathUtil::IsPowerOf2(alignment), "Alignment is not a power of two!");
    return (addr + alignment - 1) & ~(alignment - 1);
  }

  /**
   * Return the number of bytes needed to make the input address have the
   * desired alignment
   * @param addr The address to align
   * @param alignment The desired alignment
   * @return The number of bytes required to adjust the input address to the
   * desired alignment
   */
  static constexpr uintptr_t AlignmentAdjustment(uintptr_t addr, std::size_t alignment) {
    return MathUtil::AlignAddress(addr, alignment) - addr;
  }

  /**
   * Compute the factorial of a given number @em num.
   * @param num The input number.
   * @return The factorial of the input number.
   */
  static uint64_t Factorial(uint64_t num) {
    uint64_t result = 1;
    for (uint64_t i = 1; i <= num; i++) {
      result *= i;
    }
    return result;
  }

  /**
   * Are the two input 32-bit floating point values equal given the tolerances on this machine?
   * @param left The first input value.
   * @param right The second input value.
   * @return True if they're equal; false otherwise.
   */
  static bool ApproxEqual(float left, float right);

  /**
   * Are the two input 64-bit floating point values equal given the tolerances on this machine?
   * @param left The first input value.
   * @param right The second input value.
   * @return True if they're equal; false otherwise.
   */
  static bool ApproxEqual(double left, double right);
};

}  // namespace noisepage::common
