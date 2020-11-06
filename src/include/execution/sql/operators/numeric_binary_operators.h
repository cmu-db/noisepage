#pragma once

#include <cmath>

#include "common/macros.h"
#include "execution/util/arithmetic_overflow.h"

namespace noisepage::execution::sql {

// This file contains function objects that implement simple arithmetic operations.

/**
 * Function object for performing addition.
 * @tparam T The types of the input arguments to the operation.
 */
template <typename T>
struct Add {
  /**
   * @return The result of a+b;
   */
  constexpr T operator()(T a, T b) const { return a + b; }
};

/**
 * Function object for performing addition with overflow-checking.
 * @tparam T The types of the input arguments to the operation.
 */
template <typename T>
struct AddWithOverflow {
  /**
   * @return True if a+b overflows; false otherwise. @em result stores the result regardless.
   */
  constexpr bool operator()(T a, T b, T *result) const { return util::ArithmeticOverflow::Add(a, b, result); }
};

/**
 * Function object performing subtraction.
 * @tparam T The types of the input arguments to the operation.
 */
template <typename T>
struct Subtract {
  /**
   * @return The result of a-b;
   */
  constexpr T operator()(T a, T b) const { return a - b; }
};

/**
 * Function object performing subtraction with overflow-checking.
 * @tparam T The types of the input arguments to the operation.
 */
template <typename T>
struct SubtractWithOverflow {
  /**
   * @return True if a-b overflows; false otherwise. @em result stores the result regardless.
   */
  constexpr bool operator()(T a, T b, T *result) const { return util::ArithmeticOverflow::Sub(a, b, result); }
};

/**
 * Function object performing multiplication.
 * @tparam T The types of the input arguments to the operation.
 */
template <typename T>
struct Multiply {
  /**
   * @return The result of a*b;
   */
  constexpr T operator()(T a, T b) const { return a * b; }
};

/**
 * Function object performing multiplication with overflow-checking.
 * @tparam T The types of the input arguments to the operation.
 */
template <typename T>
struct MultiplyWithOverflow {
  /**
   * @return True if a*b overflows; false otherwise. @em result stores the result regardless.
   */
  constexpr bool operator()(T a, T b, T *result) const { return util::ArithmeticOverflow::Mul(a, b, result); }
};

/**
 * Function object performing division.
 * @tparam T The types of the input arguments to the operation.
 */
template <typename T>
struct Divide {
  /**
   * @return The result of a/b;
   */
  constexpr T operator()(T a, T b) const {
    NOISEPAGE_ASSERT(b != 0, "Divide by zero");  // Assumed to have checked earlier.
    return a / b;
  }
};

/**
 * Function object performing modulo.
 * @tparam T The types of the input arguments to the operation.
 */
template <typename T, typename Enable = void>
struct Modulo {
  /**
   * @return The result of a%b;
   */
  constexpr T operator()(T a, T b) const {
    NOISEPAGE_ASSERT(b != 0, "Divide by zero");  // Assumed to have checked earlier.
    return a % b;
  }
};

/**
 * Function object specialized for performing floating-point modulo.
 * @tparam T The types of the input arguments to the operation.
 */
template <typename T>
struct Modulo<T, std::enable_if_t<std::is_floating_point_v<T>>> {
  /**
   * @return The result of a%b;
   */
  constexpr T operator()(T a, T b) const {
    NOISEPAGE_ASSERT(b != 0, "Divide by zero");  // Assumed to have checked earlier.
    return std::fmod(a, b);
  }
};

}  // namespace noisepage::execution::sql
