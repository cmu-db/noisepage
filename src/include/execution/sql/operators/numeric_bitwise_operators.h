#pragma once

namespace noisepage::execution::sql {

/**
 * Bitwise AND two elements.
 */
template <typename T>
struct BitwiseAnd {
  /** @return a & b. */
  constexpr T operator()(const T &a, const T &b) const noexcept { return a & b; }
};

/**
 * Bitwise OR two elements.
 */
template <typename T>
struct BitwiseOr {
  /** @return a | b. */
  constexpr T operator()(const T &a, const T &b) const noexcept { return a | b; }
};

/**
 * Left-shift a numeric element.
 */
template <typename T>
struct BitwiseShiftLeft {
  /** @return a << b. */
  constexpr T operator()(const T &a, const T &b) const noexcept { return a << b; }
};

/**
 * Right-shift a numeric element.
 */
template <typename T>
struct BitwiseShiftRight {
  /** @return a >> b. */
  constexpr T operator()(const T &a, const T &b) const noexcept { return a >> b; }
};

/**
 * Bitwise negate a numeric element.
 */
template <typename T>
struct BitwiseNot {
  /** @return ~input. */
  constexpr T operator()(const T &input) const noexcept { return ~input; }
};

/**
 * Bitwise XOR a numeric element.
 */
template <typename T>
struct BitwiseXor {
  /** @return a ^ b. */
  constexpr T operator()(const T &a, const T &b) const noexcept { return a ^ b; }
};

}  // namespace noisepage::execution::sql
