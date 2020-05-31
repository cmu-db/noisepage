#pragma once

#include <algorithm>
#include <cstring>

#include "execution/sql/operators/comparison_operators.h"
#include "execution/sql/runtime_types.h"
#include "execution/sql/value.h"

namespace terrier::execution::sql {

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
  using SymmetricOp = NotEqual<T>;

  /**
   * @return True if left != right; false otherwise.
   */
  constexpr bool operator()(T left, T right) const { return left != right; }
};

}  // namespace terrier::execution::sql
