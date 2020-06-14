#pragma once

namespace terrier::execution::sql {

/**
 * Bitwise AND two elements.
 */
template <typename T>
struct BitwiseAnd {
  constexpr T operator()(const T &a, const T &b) const noexcept { return a & b; }
};

/**
 * Bitwise OR two elements.
 */
template <typename T>
struct BitwiseOr {
  constexpr T operator()(const T &a, const T &b) const noexcept { return a | b; }
};

/**
 * Left-shift a numeric element.
 */
template <typename T>
struct BitwiseShiftLeft {
  constexpr T operator()(const T &a, const T &b) const noexcept { return a << b; }
};

/**
 * Right-shift a numeric element.
 */
template <typename T>
struct BitwiseShiftRight {
  constexpr T operator()(const T &a, const T &b) const noexcept { return a >> b; }
};

/**
 * Bitwise negate a numeric element.
 */
template <typename T>
struct BitwiseNot {
  constexpr T operator()(const T &input) const noexcept { return ~input; }
};

/**
 * Bitwise XOR a numeric element.
 */
template <typename T>
struct BitwiseXor {
  constexpr T operator()(const T &a, const T &b) const noexcept { return a ^ b; }
};

}  // namespace terrier::execution::sql
