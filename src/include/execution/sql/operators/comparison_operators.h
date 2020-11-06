#pragma once

#include <algorithm>
#include <cstring>

#include "execution/sql/operators/comparison_operators.h"
#include "execution/sql/runtime_types.h"
#include "execution/sql/value.h"

namespace noisepage::execution::sql {

// Forward-declare all comparisons since they're used before defined.
// clang-format off
template <typename> struct Equal;
template <typename> struct GreaterThan;
template <typename> struct GreaterThanEqual;
template <typename> struct LessThan;
template <typename> struct LessThanEqual;
template <typename> struct NotEqual;
// clang-format on

/**
 * Equality operator.
 */
template <typename T>
struct Equal {
  /** The symmetric operation for equality is itself. */
  using SymmetricOp = Equal<T>;

  /**
   * @return True if left == right; false otherwise.
   */
  constexpr bool operator()(T left, T right) const { return left == right; }
};

/**
 * Greater-than operator.
 */
template <typename T>
struct GreaterThan {
  /** The symmetric operation for greater than is less than. */
  using SymmetricOp = LessThan<T>;

  /**
   * @return True if left > right; false otherwise.
   */
  constexpr bool operator()(T left, T right) const { return left > right; }
};

/**
 * Greater-than or equal operator.
 */
template <typename T>
struct GreaterThanEqual {
  /** The symmetric operation for greater than equal is less than equal. */
  using SymmetricOp = LessThanEqual<T>;

  /**
   * @return True if left >= right; false otherwise.
   */
  constexpr bool operator()(T left, T right) const { return left >= right; }
};

/**
 * Less-than operator.
 */
template <typename T>
struct LessThan {
  /** The symmetric operation for less than is greater than. */
  using SymmetricOp = GreaterThan<T>;

  /**
   * @return True if left < right; false otherwise.
   */
  constexpr bool operator()(T left, T right) const { return left < right; }
};

/**
 * Less-than or equal operator.
 */
template <typename T>
struct LessThanEqual {
  /** The symmetric operation for less than equal is greater than equal. */
  using SymmetricOp = GreaterThanEqual<T>;

  /**
   * @return True if left <= right; false otherwise.
   */
  constexpr bool operator()(T left, T right) const { return left <= right; }
};

/**
 * Inequality operator.
 */
template <typename T>
struct NotEqual {
  /** The symmetric operation for not equal is not equal. */
  using SymmetricOp = NotEqual<T>;

  /**
   * @return True if left != right; false otherwise.
   */
  constexpr bool operator()(T left, T right) const { return left != right; }
};

}  // namespace noisepage::execution::sql
