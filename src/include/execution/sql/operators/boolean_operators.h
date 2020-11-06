#pragma once

namespace noisepage::execution::sql {

/**
 * Boolean negation.
 */
struct Not {
  /** @return True if NOT input. */
  bool operator()(const bool input) const noexcept { return !input; }
};

/**
 * Boolean AND.
 */
struct And {
  /** @return True if left AND right. */
  bool operator()(const bool left, bool right) const noexcept { return left && right; }
};

/**
 * Determine if the result of a boolean AND is NULL. The truth table for AND is:
 *
 * true  AND true  = true
 * true  AND false = false
 * true  AND NULL  = NULL
 * false AND true  = false
 * false AND false = false
 * false AND NULL  = false
 * NULL  AND true  = NULL
 * NULL  AND false = false
 * NULL  AND NULL  = NULL
 *
 * So, the result of an AND is null if:
 * (1) Both inputs are NULL, or
 * (2) Either input is true and the other is NULL.
 */
struct AndNullMask {
  /** @return True (i.e. result is NULL) if both inputs are NULL, or either input is true and the other is NULL. */
  bool operator()(const bool left, const bool right, const bool left_null, const bool right_null) const noexcept {
    return (left_null && (right_null || right)) || (left && right_null);
  }
};

/**
 * Boolean OR.
 */
struct Or {
  /** @return True if left OR right. */
  bool operator()(const bool left, const bool right) const noexcept { return left || right; }
};

/**
 * Determine if the result of a boolean OR is NULL. The truth table for OR is:
 *
 * true  OR true  = true
 * true  OR false = true
 * true  OR NULL  = true
 * false OR true  = true
 * false OR false = false
 * false OR NULL  = NULL
 * NULL  OR true  = true
 * NULL  OR false = NULL
 * NULL  OR NULL  = NULL
 *
 * So, the result of an OR is null if:
 * (1) Both inputs are NULL, or
 * (2) Either input is false and the other is NULL.
 */
struct OrNullMask {
  /** @return True (i.e. result is NULL) if both inputs are NULL, or either input is false and the other is NULL. */
  bool operator()(const bool left, const bool right, const bool left_null, const bool right_null) const noexcept {
    return (left_null && (right_null || !right)) || (!left && right_null);
  }
};

}  // namespace noisepage::execution::sql
