#pragma once

#include <limits>

#include "execution/util/common.h"

namespace terrier::execution::util {

/**
 * Utility class to handle arithmetic operations that can overflow.
 */
class ArithmeticOverflow {
 public:
  /**
   * Minimum i128 value
   */
  static constexpr i128 kMinInt128 = std::numeric_limits<i128>::min();
  /**
   * Maximum i128 value
   */
  static constexpr i128 kMaxInt128 = std::numeric_limits<i128>::max();

  // Deleted to force only static functions
  ArithmeticOverflow() = delete;

  // -------------------------------------------------------
  // Addition
  // -------------------------------------------------------

  /**
   * Add two integral values and store their result in @em res. Return true if
   * the addition overflowed.
   * @tparam T The types of the input and output.
   * @param a The first operand.
   * @param b The second operand.
   * @param[out] res Where the result of the addition is written to.
   * @return True if the addition overflowed; false otherwise.
   */
  template <typename T>
  static bool Add(T a, T b, T *res) {
    return __builtin_add_overflow(a, b, res);
  }

  /**
   * Add two signed 32-bit integer values and store their result in @em res.
   * Return true if the addition overflowed.
   * @param a The first operand.
   * @param b The second operand.
   * @param[out] res Where the result of the addition is written to.
   * @return True if the addition overflowed; false otherwise.
   */
  static bool Add(const i32 a, const i32 b, i32 *res) { return __builtin_sadd_overflow(a, b, res); }

  /**
   * Add two signed 64-bit integer values and store their result in @em res.
   * Return true if the addition overflowed.
   * @param a The first operand.
   * @param b The second operand.
   * @param[out] res Where the result of the addition is written to.
   * @return True if the addition overflowed; false otherwise.
   */
  static bool Add(const i64 a, const i64 b, i64 *res) { return __builtin_saddl_overflow(a, b, res); }

  /**
   * Add two signed 128-bit integer values and store their result in @em res.
   * Return true if the addition overflowed.
   * @param a The first operand.
   * @param b The second operand.
   * @param[out] res Where the result of the addition is written to.
   * @return True if the addition overflowed; false otherwise.
   */
  static bool Add(const i128 a, const i128 b, i128 *res) {
    *res = a + b;
    return (b > 0 && a > kMaxInt128 - b) || (b < 0 && a < kMinInt128 - b);
  }

  /**
   * Add two unsigned 32-bit integer values and store their result in @em res.
   * Return true if the addition overflowed.
   * @param a The first operand.
   * @param b The second operand.
   * @param[out] res Where the result of the addition is written to.
   * @return True if the addition overflowed; false otherwise.
   */
  static bool Add(const u32 a, const u32 b, u32 *res) { return __builtin_uadd_overflow(a, b, res); }

  /**
   * Add two unsigned 64-bit integer values and store their result in @em res.
   * Return true if the addition overflowed.
   * @param a The first operand.
   * @param b The second operand.
   * @param[out] res Where the result of the addition is written to.
   * @return True if the addition overflowed; false otherwise.
   */
  static bool Add(const u64 a, const u64 b, u64 *res) { return __builtin_uaddl_overflow(a, b, res); }

  /**
   * Add two unsigned 64-bit integer values and store their result in @em res.
   * Return true if the addition overflowed.
   * @param a The first operand.
   * @param b The second operand.
   * @param[out] res Where the result of the addition is written to.
   * @return True if the addition overflowed; false otherwise.
   */
  static bool Add(const u128 a, const u128 b, u128 *res) {
    *res = a + b;
    return (a > kMaxInt128 - b);
  }

  // -------------------------------------------------------
  // Subtraction
  // -------------------------------------------------------

  /**
   * Subtract two integer values and store their result in @em res. Return true
   * if the subtraction overflowed.
   * @param a The first operand.
   * @param b The second operand.
   * @param[out] res Where the result of the subtraction is written to.
   * @return True if the subtraction overflowed; false otherwise.
   */
  template <typename T>
  static bool Sub(T a, T b, T *res) {
    return __builtin_sub_overflow(a, b, res);
  }

  /**
   * Subtract two signed 32-bit values and store their result in @em res. Return
   * true if the subtraction overflowed.
   * @param a The first operand.
   * @param b The second operand.
   * @param[out] res Where the result of the subtraction is written to.
   * @return True if the subtraction overflowed; false otherwise.
   */
  static bool Sub(const i32 a, const i32 b, i32 *res) { return __builtin_ssub_overflow(a, b, res); }

  /**
   * Subtract two signed 64-bit values and store their result in @em res. Return
   * true if the subtraction overflowed.
   * @param a The first operand.
   * @param b The second operand.
   * @param[out] res Where the result of the subtraction is written to.
   * @return True if the subtraction overflowed; false otherwise.
   */
  static bool Sub(const i64 a, const i64 b, i64 *res) { return __builtin_ssubl_overflow(a, b, res); }

  /**
   * Subtract two signed 128-bit values and store their result in @em res.
   * Return true if the subtraction overflowed.
   * @param a The first operand.
   * @param b The second operand.
   * @param[out] res Where the result of the subtraction is written to.
   * @return True if the subtraction overflowed; false otherwise.
   */
  static bool Sub(const i128 a, const i128 b, i128 *res) {
    *res = a - b;
    return (b > 0 && a < kMinInt128 + b) || (b < 0 && a > kMaxInt128 + b);
  }

  /**
   * Subtract two unsigned 32-bit values and store their result in @em res.
   * Return true if the subtraction overflowed.
   * @param a The first operand.
   * @param b The second operand.
   * @param[out] res Where the result of the subtraction is written to.
   * @return True if the subtraction overflowed; false otherwise.
   */
  static bool Sub(const u32 a, const u32 b, u32 *res) { return __builtin_usub_overflow(a, b, res); }

  /**
   * Subtract two unsigned 64-bit values and store their result in @em res.
   * Return true if the subtraction overflowed.
   * @param a The first operand.
   * @param b The second operand.
   * @param[out] res Where the result of the subtraction is written to.
   * @return True if the subtraction overflowed; false otherwise.
   */
  static bool Sub(const u64 a, const u64 b, u64 *res) { return __builtin_usubl_overflow(a, b, res); }

  /**
   * Subtract two unsigned 128-bit values and store their result in @em res.
   * Return true if the subtraction overflowed.
   * @param a The first operand.
   * @param b The second operand.
   * @param[out] res Where the result of the subtraction is written to.
   * @return True if the subtraction overflowed; false otherwise.
   */
  static bool Sub(const u128 a, const u128 b, u128 *res) {
    *res = a + b;
    return (a < kMinInt128 + b);
  }

  // -------------------------------------------------------
  // Multiplication
  // -------------------------------------------------------

  /**
   * Multiply two integer values and store their result in @em res. Return true
   * if the multiplication overflowed.
   * @param a The first operand.
   * @param b The second operand.
   * @param[out] res Where the result of the multiplication is written to.
   * @return True if the subtraction overflowed; false otherwise.sql
   */
  template <typename T>
  static bool Mul(T a, T b, T *res) {
    return __builtin_mul_overflow(a, b, res);
  }

  /**
   * Multiply two signed 32-bit integer values and store their result in @em
   * res. Return true if the multiplication overflowed.
   * @param a The first operand.
   * @param b The second operand.
   * @param[out] res Where the result of the multiplication is written to.
   * @return True if the subtraction overflowed; false otherwise.
   */
  static bool Mul(const i32 a, const i32 b, i32 *res) { return __builtin_smul_overflow(a, b, res); }

  /**
   * Multiply two signed 64-bit integer values and store their result in @em
   * res. Return true if the multiplication overflowed.
   * @param a The first operand.
   * @param b The second operand.
   * @param[out] res Where the result of the multiplication is written to.
   * @return True if the subtraction overflowed; false otherwise.
   */
  static bool Mul(const i64 a, const i64 b, i64 *res) { return __builtin_smull_overflow(a, b, res); }

  /**
   * Multiply two signed 128-bit integer values and store their result in @em
   * res. Return true if the multiplication overflowed.
   * @param a The first operand.
   * @param b The second operand.
   * @param[out] res Where the result of the multiplication is written to.
   * @return True if the subtraction overflowed; false otherwise.
   */
  static bool Mul(const i128 a, const i128 b, i128 *res) {
    *res = static_cast<const u128>(a) * static_cast<const u128>(b);
    if (a == 0 || b == 0) {
      return false;
    }

    return (a * b) / b != a;
  }

  /**
   * Multiply two unsigned 32-bit integer values and store their result in @em
   * res. Return true if the multiplication overflowed.
   * @param a The first operand.
   * @param b The second operand.
   * @param[out] res Where the result of the multiplication is written to.
   * @return True if the subtraction overflowed; false otherwise.
   */
  static bool Mul(const u32 a, const u32 b, u32 *res) { return __builtin_umul_overflow(a, b, res); }

  /**
   * Multiply two unsigned 64-bit integer values and store their result in @em
   * res. Return true if the multiplication overflowed.
   * @param a The first operand.
   * @param b The second operand.
   * @param[out] res Where the result of the multiplication is written to.
   * @return True if the subtraction overflowed; false otherwise.
   */
  static bool Mul(const u64 a, const u64 b, u64 *res) { return __builtin_umull_overflow(a, b, res); }

  /**
   * Multiply two unsigned 128-bit integer values and store their result in @em
   * res. Return true if the multiplication overflowed.
   * @param a The first operand.
   * @param b The second operand.
   * @param[out] res Where the result of the multiplication is written to.
   * @return True if the subtraction overflowed; false otherwise.
   */
  static bool Mul(const u128 a, const u128 b, u128 *res) {
    *res = a * b;
    return (a * b) / b != a;
  }
};

}  // namespace terrier::execution::util
