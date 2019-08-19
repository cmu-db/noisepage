#pragma once

#include <cstdint>
#include <cstdlib>

#include "llvm/Support/MathExtras.h"

#include "common/macros.h"
#include "execution/util/execution_common.h"

namespace terrier::common {

/**
 * Utility class containing various math/arithmetic functions
 */
class MathUtil {
 public:
  /**
   * Perform a division taking the ceil of the result
   * @param numerator The numerator
   * @param denominator The denominator
   * @return The result of the division rounded up to the next integer value
   */
  static uint64_t DivRoundUp(uint64_t numerator, uint64_t denominator) {
    return (numerator + denominator - 1) / denominator;
  }

  /**
   * Return true if the input value is a power of two > 0
   * @param val The value to check
   * @return True if the value is a power of two > 0
   */
  static constexpr bool IsPowerOf2(uint64_t val) { return llvm::isPowerOf2_64(val); }

  /**
   * Compute the next power of two strictly greater than the input @em val
   */
  static uint64_t NextPowerOf2(uint64_t val) { return llvm::NextPowerOf2(val); }

  /**
   * Return the next power of two greater than or equal to the input @em val
   */
  static uint64_t PowerOf2Ceil(uint64_t val) { return llvm::PowerOf2Ceil(val); }

  /**
   * Compute the power of tww loweer than the provided input @em val
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
  static bool IsAligned(uint64_t value, uint64_t alignment) {
    TERRIER_ASSERT(alignment != 0u && IsPowerOf2(alignment), "Align must be a non-zero power of two.");
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
  static bool IsAlignedGeneric(uint64_t value, uint64_t alignment) {
    TERRIER_ASSERT(alignment != 0u, "Align must be non-zero.");
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
    TERRIER_ASSERT(alignment > 0 && MathUtil::IsPowerOf2(alignment), "Alignment is not a power of two!");
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
};

}  // namespace terrier::common
