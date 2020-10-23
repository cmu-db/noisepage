#pragma once

#include <algorithm>

#include "execution/sql/operators/comparison_operators.h"
#include "execution/sql/value.h"

namespace noisepage::execution::sql {

/**
 * Comparison functions for SQL values.
 */
class EXPORT ComparisonFunctions {
 public:
  /** This class cannot be instantiated. */
  DISALLOW_INSTANTIATION(ComparisonFunctions);

  /**
   * Sets result = (v1 == v2)
   */
  static void EqBoolVal(BoolVal *result, const BoolVal &v1, const BoolVal &v2);

  /**
   * Sets result = (v1 >= v2)
   */
  static void GeBoolVal(BoolVal *result, const BoolVal &v1, const BoolVal &v2);

  /**
   * Sets result = (v1 > v2)
   */
  static void GtBoolVal(BoolVal *result, const BoolVal &v1, const BoolVal &v2);

  /**
   * Sets result = (v1 <= v2)
   */
  static void LeBoolVal(BoolVal *result, const BoolVal &v1, const BoolVal &v2);

  /**
   * Sets result = (v1 < v2)
   */
  static void LtBoolVal(BoolVal *result, const BoolVal &v1, const BoolVal &v2);

  /**
   * Sets result = (v1 != v2)
   */
  static void NeBoolVal(BoolVal *result, const BoolVal &v1, const BoolVal &v2);

  /**
   * Sets result = (v1 == v2)
   */
  static void EqInteger(BoolVal *result, const Integer &v1, const Integer &v2);

  /**
   * Sets result = (v1 >= v2)
   */
  static void GeInteger(BoolVal *result, const Integer &v1, const Integer &v2);

  /**
   * Sets result = (v1 > v2)
   */
  static void GtInteger(BoolVal *result, const Integer &v1, const Integer &v2);

  /**
   * Sets result = (v1 <= v2)
   */
  static void LeInteger(BoolVal *result, const Integer &v1, const Integer &v2);

  /**
   * Sets result = (v1 < v2)
   */
  static void LtInteger(BoolVal *result, const Integer &v1, const Integer &v2);

  /**
   * Sets result = (v1 != v2)
   */
  static void NeInteger(BoolVal *result, const Integer &v1, const Integer &v2);

  /**
   * Sets result = (v1 == v2)
   */
  static void EqReal(BoolVal *result, const Real &v1, const Real &v2);

  /**
   * Sets result = (v1 >= v2)
   */
  static void GeReal(BoolVal *result, const Real &v1, const Real &v2);

  /**
   * Sets result = (v1 > v2)
   */
  static void GtReal(BoolVal *result, const Real &v1, const Real &v2);

  /**
   * Sets result = (v1 <= v2)
   */
  static void LeReal(BoolVal *result, const Real &v1, const Real &v2);

  /**
   * Sets result = (v1 < v2)
   */
  static void LtReal(BoolVal *result, const Real &v1, const Real &v2);

  /**
   * Sets result = (v1 != v2)
   */
  static void NeReal(BoolVal *result, const Real &v1, const Real &v2);

  /**
   * Sets result = (v1 == v2)
   */
  static void EqStringVal(BoolVal *result, const StringVal &v1, const StringVal &v2);

  /**
   * Sets result = (v1 >= v2)
   */
  static void GeStringVal(BoolVal *result, const StringVal &v1, const StringVal &v2);

  /**
   * Sets result = (v1 > v2)
   */
  static void GtStringVal(BoolVal *result, const StringVal &v1, const StringVal &v2);

  /**
   * Sets result = (v1 <= v2)
   */
  static void LeStringVal(BoolVal *result, const StringVal &v1, const StringVal &v2);

  /**
   * Sets result = (v1 < v2)
   */
  static void LtStringVal(BoolVal *result, const StringVal &v1, const StringVal &v2);

  /**
   * Sets result = (v1 != v2)
   */
  static void NeStringVal(BoolVal *result, const StringVal &v1, const StringVal &v2);

  /**
   * Sets result = (v1 == v2)
   */
  static void EqDateVal(BoolVal *result, const DateVal &v1, const DateVal &v2);

  /**
   * Sets result = (v1 >= v2)
   */
  static void GeDateVal(BoolVal *result, const DateVal &v1, const DateVal &v2);

  /**
   * Sets result = (v1 > v2)
   */
  static void GtDateVal(BoolVal *result, const DateVal &v1, const DateVal &v2);

  /**
   * Sets result = (v1 <= v2)
   */
  static void LeDateVal(BoolVal *result, const DateVal &v1, const DateVal &v2);

  /**
   * Sets result = (v1 < v2)
   */
  static void LtDateVal(BoolVal *result, const DateVal &v1, const DateVal &v2);

  /**
   * Sets result = (v1 != v2)
   */
  static void NeDateVal(BoolVal *result, const DateVal &v1, const DateVal &v2);

  /**
   * Sets result = (v1 == v2)
   */
  static void EqTimestampVal(BoolVal *result, const TimestampVal &v1, const TimestampVal &v2);

  /**
   * Sets result = (v1 >= v2)
   */
  static void GeTimestampVal(BoolVal *result, const TimestampVal &v1, const TimestampVal &v2);

  /**
   * Sets result = (v1 > v2)
   */
  static void GtTimestampVal(BoolVal *result, const TimestampVal &v1, const TimestampVal &v2);

  /**
   * Sets result = (v1 <= v2)
   */
  static void LeTimestampVal(BoolVal *result, const TimestampVal &v1, const TimestampVal &v2);

  /**
   * Sets result = (v1 < v2)
   */
  static void LtTimestampVal(BoolVal *result, const TimestampVal &v1, const TimestampVal &v2);

  /**
   * Sets result = (v1 != v2)
   */
  static void NeTimestampVal(BoolVal *result, const TimestampVal &v1, const TimestampVal &v2);

  /**
   * Compare two raw strings. Returns:
   * < 0 if s1 < s2
   * 0 if s1 == s2
   * > 0 if s1 > s2
   *
   * @param s1 The first string.
   * @param len1 The length of the first string.
   * @param s2 The second string.
   * @param len2 The length of the second string.
   * @param min_len The minimum length between the two input strings.
   * @return The appropriate signed value indicating comparison order.
   */
  static int32_t RawStringCompare(const char *s1, std::size_t len1, const char *s2, std::size_t len2,
                                  std::size_t min_len) {
    const auto result = (min_len == 0) ? 0 : std::memcmp(s1, s2, min_len);
    if (result != 0) {
      return result;
    }
    return int32_t(len1) - int32_t(len2);
  }

  /**
   * Logical negation for SQL booleans
   */
  static void NotBoolVal(BoolVal *result, const BoolVal &input) {
    result->is_null_ = input.is_null_;
    result->val_ = !input.val_;
  }

 private:
  /**
   * Compare two strings. Returns
   * < 0 if s1 < s2
   * 0 if s1 == s2
   * > 0 if s1 > s2
   *
   * @param v1 The first string.
   * @param v2 The second string.
   * @return The appropriate signed value indicating comparison order.
   */
  static int32_t Compare(const StringVal &v1, const StringVal &v2) {
    NOISEPAGE_ASSERT(!v1.is_null_ && !v2.is_null_, "Both input strings must not be null");
    const auto min_len = std::min(v1.GetLength(), v2.GetLength());
    if (min_len == 0) {
      if (v1.GetLength() == v2.GetLength()) {
        return 0;
      }
      if (v1.GetLength() == 0) {
        return -1;
      }
      return 1;
    }
    return RawStringCompare(v1.GetContent(), v1.GetLength(), v2.GetContent(), v2.GetLength(), min_len);
  }
};

// ---------------------------------------------------------
// Implementation below
// ---------------------------------------------------------

// The functions below are inlined in the header for performance. Don't move it
// unless you know what you're doing.

#define BINARY_COMPARISON_NUMERIC_FN_HIDE_NULL(NAME, TYPE, OP)                                   \
  inline void ComparisonFunctions::NAME##TYPE(BoolVal *result, const TYPE &v1, const TYPE &v2) { \
    using CppType = decltype(v1.val_);                                                           \
    result->is_null_ = (v1.is_null_ || v2.is_null_);                                             \
    result->val_ = OP<CppType>{}(v1.val_, v2.val_);                                              \
  }

#define BINARY_COMPARISON_STRING_FN_HIDE_NULL(NAME, TYPE, OP)                                              \
  inline void ComparisonFunctions::NAME##TYPE(BoolVal *result, const StringVal &v1, const StringVal &v2) { \
    using CppType = decltype(v1.val_);                                                                     \
    if (v1.is_null_ || v2.is_null_) {                                                                      \
      *result = BoolVal::Null();                                                                           \
      return;                                                                                              \
    }                                                                                                      \
    *result = BoolVal(OP<CppType>{}(v1.val_, v2.val_));                                                    \
  }

#define BINARY_COMPARISONS(NAME, OP)                             \
  BINARY_COMPARISON_NUMERIC_FN_HIDE_NULL(NAME, BoolVal, OP)      \
  BINARY_COMPARISON_NUMERIC_FN_HIDE_NULL(NAME, Integer, OP)      \
  BINARY_COMPARISON_NUMERIC_FN_HIDE_NULL(NAME, Real, OP)         \
  BINARY_COMPARISON_NUMERIC_FN_HIDE_NULL(NAME, DateVal, OP)      \
  BINARY_COMPARISON_NUMERIC_FN_HIDE_NULL(NAME, TimestampVal, OP) \
  BINARY_COMPARISON_STRING_FN_HIDE_NULL(NAME, StringVal, OP)

BINARY_COMPARISONS(Eq, noisepage::execution::sql::Equal);
BINARY_COMPARISONS(Ge, noisepage::execution::sql::GreaterThanEqual);
BINARY_COMPARISONS(Gt, noisepage::execution::sql::GreaterThan);
BINARY_COMPARISONS(Le, noisepage::execution::sql::LessThanEqual);
BINARY_COMPARISONS(Lt, noisepage::execution::sql::LessThan);
BINARY_COMPARISONS(Ne, noisepage::execution::sql::NotEqual);

#undef BINARY_COMPARISONS
#undef BINARY_COMPARISON_STRING_FN_HIDE_NULL
#undef BINARY_COMPARISON_NUMERIC_FN_HIDE_NULL
}  // namespace noisepage::execution::sql
