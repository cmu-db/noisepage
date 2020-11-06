#pragma once

#include <cmath>

#include "execution/sql/operators/numeric_binary_operators.h"
#include "execution/sql/operators/numeric_operators.h"
#include "execution/sql/value.h"
#include "execution/util/arithmetic_overflow.h"

namespace noisepage::execution::sql {

/**
 * Utility class to handle various arithmetic SQL functions.
 */
class EXPORT ArithmeticFunctions {
 public:
  /** This class cannot be instantiated. */
  DISALLOW_INSTANTIATION(ArithmeticFunctions);

  /**
   * Integer addition
   */
  static void Add(Integer *result, const Integer &a, const Integer &b);

  /**
   * Integer addition with overflow check
   */
  static void Add(Integer *result, const Integer &a, const Integer &b, bool *overflow);

  /**
   * Real addition
   */
  static void Add(Real *result, const Real &a, const Real &b);

  /**
   * Integer subtraction
   */
  static void Sub(Integer *result, const Integer &a, const Integer &b);

  /**
   * Integer subtraction with overflow check
   */
  static void Sub(Integer *result, const Integer &a, const Integer &b, bool *overflow);

  /**
   * Real addition
   */
  static void Sub(Real *result, const Real &a, const Real &b);

  /**
   * Integer multiplication
   */
  static void Mul(Integer *result, const Integer &a, const Integer &b);

  /**
   * Integer multiplication with overflow check
   */
  static void Mul(Integer *result, const Integer &a, const Integer &b, bool *overflow);

  /**
   * Real multiplication
   */
  static void Mul(Real *result, const Real &a, const Real &b);

  /**
   * Integer division
   */
  static void IntDiv(Integer *result, const Integer &a, const Integer &b, bool *div_by_zero);

  /**
   * Integer real
   */
  static void Div(Real *result, const Real &a, const Real &b, bool *div_by_zero);

  /**
   * Integer modulo
   */
  static void IntMod(Integer *result, const Integer &a, const Integer &b, bool *div_by_zero);

  /**
   * Real modulo
   */
  static void Mod(Real *result, const Real &a, const Real &b, bool *div_by_zero);

  /**
   * Return PI
   */
  static void Pi(Real *result);

  /**
   * Return exp(1)
   */
  static void E(Real *result);

  /**
   * Absolute integer value
   */
  static void Abs(Integer *result, const Integer &v);

  /**
   * Absolute real value
   */
  static void Abs(Real *result, const Real &v);

  /**
   * Trig sin
   */
  static void Sin(Real *result, const Real &v);

  /**
   * Trig asin
   */
  static void Asin(Real *result, const Real &v);

  /**
   * Trig cos
   */
  static void Cos(Real *result, const Real &v);

  /**
   * Trig acos
   */
  static void Acos(Real *result, const Real &v);

  /**
   * Trig tan
   */
  static void Tan(Real *result, const Real &v);

  /**
   * Trig cot
   */
  static void Cot(Real *result, const Real &v);

  /**
   * Trig atan
   */
  static void Atan(Real *result, const Real &v);

  /**
   * Trig atan2
   */
  static void Atan2(Real *result, const Real &a, const Real &b);

  /**
   * Trig cosh
   */
  static void Cosh(Real *result, const Real &v);

  /**
   * Trig tanh
   */
  static void Tanh(Real *result, const Real &v);

  /**
   * Trig sinh
   */
  static void Sinh(Real *result, const Real &v);

  /**
   * Square root
   */
  static void Sqrt(Real *result, const Real &v);

  /**
   * Cube root
   */
  static void Cbrt(Real *result, const Real &v);

  /**
   * Exponential
   */
  static void Exp(Real *result, const Real &v);

  /**
   * Ceil
   */
  static void Ceil(Real *result, const Real &v);

  /**
   * Floor
   */
  static void Floor(Real *result, const Real &v);

  /**
   * Truncations
   */
  static void Truncate(Real *result, const Real &v);

  /**
   * Natural Log
   */
  static void Ln(Real *result, const Real &v);

  /**
   * Log base 2
   */
  static void Log2(Real *result, const Real &v);

  /**
   * Log base 10
   */
  static void Log10(Real *result, const Real &v);

  /**
   * Return the sign
   */
  static void Sign(Real *result, const Real &v);

  /**
   * Convert to radians
   */
  static void Radians(Real *result, const Real &v);

  /**
   * Convert to degree
   */
  static void Degrees(Real *result, const Real &v);

  /**
   * Round to nearest
   */
  static void Round(Real *result, const Real &v);

  /**
   * Rounding with precision
   */
  static void Round2(Real *result, const Real &v, const Integer &precision);

  /**
   * Logarithm with base
   */
  static void Log(Real *result, const Real &base, const Real &val);

  /**
   * Exponentiation base ^ val
   */
  static void Pow(Real *result, const Real &base, const Real &val);

 private:
  // Cotangent
  static double Cotan(const double arg) { return (1.0 / std::tan(arg)); }
};

// ---------------------------------------------------------
// Implementation below
// ---------------------------------------------------------

// The functions below are inlined in the header for performance. Don't move it
// unless you know what you're doing.

#define UNARY_MATH_EXPENSIVE_HIDE_NULL(OP, RET_TYPE, INPUT_TYPE)               \
  inline void ArithmeticFunctions::OP(RET_TYPE *result, const INPUT_TYPE &v) { \
    using CppType = decltype(v.val_);                                          \
    if (v.is_null_) {                                                          \
      *result = RET_TYPE::Null();                                              \
      return;                                                                  \
    }                                                                          \
    *result = RET_TYPE(noisepage::execution::sql::OP<CppType>{}(v.val_));      \
  }

#define BINARY_MATH_FAST_HIDE_NULL(NAME, RET_TYPE, INPUT_TYPE1, INPUT_TYPE2, OP)                        \
  inline void ArithmeticFunctions::NAME(RET_TYPE *result, const INPUT_TYPE1 &a, const INPUT_TYPE2 &b) { \
    using CppType = decltype(result->val_);                                                             \
    result->is_null_ = (a.is_null_ || b.is_null_);                                                      \
    result->val_ = OP<CppType>{}(a.val_, b.val_);                                                       \
  }

#define BINARY_MATH_FAST_HIDE_NULL_OVERFLOW(NAME, RET_TYPE, INPUT_TYPE1, INPUT_TYPE2, OP)             \
  inline void ArithmeticFunctions::NAME(RET_TYPE *result, const INPUT_TYPE1 &a, const INPUT_TYPE2 &b, \
                                        bool *overflow) {                                             \
    using CppType = decltype(result->val_);                                                           \
    result->is_null_ = (a.is_null_ || b.is_null_);                                                    \
    *overflow = OP<CppType>{}(a.val_, b.val_, &result->val_);                                         \
  }

#define BINARY_MATH_CHECK_ZERO_HIDE_NULL(NAME, RET_TYPE, INPUT_TYPE1, INPUT_TYPE2, OP)                \
  inline void ArithmeticFunctions::NAME(RET_TYPE *result, const INPUT_TYPE1 &a, const INPUT_TYPE2 &b, \
                                        bool *div_by_zero) {                                          \
    using CppType = decltype(result->val_);                                                           \
    if (a.is_null_ || b.is_null_ || b.val_ == 0) {                                                    \
      *div_by_zero = true;                                                                            \
      *result = RET_TYPE::Null();                                                                     \
      return;                                                                                         \
    }                                                                                                 \
    *result = RET_TYPE(OP<CppType>{}(a.val_, b.val_));                                                \
  }

BINARY_MATH_FAST_HIDE_NULL(Add, Integer, Integer, Integer, noisepage::execution::sql::Add);
BINARY_MATH_FAST_HIDE_NULL(Add, Real, Real, Real, noisepage::execution::sql::Add);
BINARY_MATH_FAST_HIDE_NULL_OVERFLOW(Add, Integer, Integer, Integer, noisepage::execution::sql::AddWithOverflow);
BINARY_MATH_FAST_HIDE_NULL(Sub, Integer, Integer, Integer, noisepage::execution::sql::Subtract);
BINARY_MATH_FAST_HIDE_NULL(Sub, Real, Real, Real, noisepage::execution::sql::Subtract);
BINARY_MATH_FAST_HIDE_NULL_OVERFLOW(Sub, Integer, Integer, Integer, noisepage::execution::sql::SubtractWithOverflow);
BINARY_MATH_FAST_HIDE_NULL(Mul, Integer, Integer, Integer, noisepage::execution::sql::Multiply);
BINARY_MATH_FAST_HIDE_NULL(Mul, Real, Real, Real, noisepage::execution::sql::Multiply);
BINARY_MATH_FAST_HIDE_NULL_OVERFLOW(Mul, Integer, Integer, Integer, noisepage::execution::sql::MultiplyWithOverflow);
BINARY_MATH_CHECK_ZERO_HIDE_NULL(IntDiv, Integer, Integer, Integer, noisepage::execution::sql::Divide);
BINARY_MATH_CHECK_ZERO_HIDE_NULL(Div, Real, Real, Real, noisepage::execution::sql::Divide);
BINARY_MATH_CHECK_ZERO_HIDE_NULL(IntMod, Integer, Integer, Integer, noisepage::execution::sql::Modulo);
BINARY_MATH_CHECK_ZERO_HIDE_NULL(Mod, Real, Real, Real, noisepage::execution::sql::Modulo);

inline void ArithmeticFunctions::Pi(Real *result) { *result = Real(M_PI); }

inline void ArithmeticFunctions::E(Real *result) { *result = Real(M_E); }

UNARY_MATH_EXPENSIVE_HIDE_NULL(Abs, Integer, Integer);
UNARY_MATH_EXPENSIVE_HIDE_NULL(Abs, Real, Real);
UNARY_MATH_EXPENSIVE_HIDE_NULL(Sin, Real, Real);
UNARY_MATH_EXPENSIVE_HIDE_NULL(Asin, Real, Real);
UNARY_MATH_EXPENSIVE_HIDE_NULL(Cos, Real, Real);
UNARY_MATH_EXPENSIVE_HIDE_NULL(Acos, Real, Real);
UNARY_MATH_EXPENSIVE_HIDE_NULL(Tan, Real, Real);
UNARY_MATH_EXPENSIVE_HIDE_NULL(Cot, Real, Real);
UNARY_MATH_EXPENSIVE_HIDE_NULL(Atan, Real, Real);
UNARY_MATH_EXPENSIVE_HIDE_NULL(Cosh, Real, Real);
UNARY_MATH_EXPENSIVE_HIDE_NULL(Tanh, Real, Real);
UNARY_MATH_EXPENSIVE_HIDE_NULL(Sinh, Real, Real);
UNARY_MATH_EXPENSIVE_HIDE_NULL(Sqrt, Real, Real);
UNARY_MATH_EXPENSIVE_HIDE_NULL(Cbrt, Real, Real);
UNARY_MATH_EXPENSIVE_HIDE_NULL(Ceil, Real, Real);
UNARY_MATH_EXPENSIVE_HIDE_NULL(Floor, Real, Real);
UNARY_MATH_EXPENSIVE_HIDE_NULL(Truncate, Real, Real);
UNARY_MATH_EXPENSIVE_HIDE_NULL(Ln, Real, Real);
UNARY_MATH_EXPENSIVE_HIDE_NULL(Log2, Real, Real);
UNARY_MATH_EXPENSIVE_HIDE_NULL(Log10, Real, Real);
UNARY_MATH_EXPENSIVE_HIDE_NULL(Exp, Real, Real);
UNARY_MATH_EXPENSIVE_HIDE_NULL(Sign, Real, Real);
UNARY_MATH_EXPENSIVE_HIDE_NULL(Radians, Real, Real);
UNARY_MATH_EXPENSIVE_HIDE_NULL(Degrees, Real, Real);
UNARY_MATH_EXPENSIVE_HIDE_NULL(Round, Real, Real);

inline void ArithmeticFunctions::Atan2(Real *result, const Real &a, const Real &b) {
  if (a.is_null_ || b.is_null_) {
    *result = Real::Null();
    return;
  }
  *result = Real(noisepage::execution::sql::Atan2<double>{}(a.val_, b.val_));
}

inline void ArithmeticFunctions::Round2(Real *result, const Real &v, const Integer &precision) {
  if (v.is_null_ || precision.is_null_) {
    *result = Real::Null();
    return;
  }
  *result = Real(noisepage::execution::sql::RoundUpTo<double, int64_t>{}(v.val_, precision.val_));
}

inline void ArithmeticFunctions::Log(Real *result, const Real &base, const Real &val) {
  if (base.is_null_ || val.is_null_) {
    *result = Real::Null();
    return;
  }
  *result = Real(noisepage::execution::sql::Log<double>{}(base.val_, val.val_));
}

inline void ArithmeticFunctions::Pow(Real *result, const Real &base, const Real &val) {
  if (base.is_null_ || val.is_null_) {
    *result = Real::Null();
    return;
  }
  *result = Real(noisepage::execution::sql::Pow<double, double>{}(base.val_, val.val_));
}

#undef BINARY_FN_CHECK_ZERO
#undef BINARY_MATH_CHECK_ZERO_HIDE_NULL
#undef BINARY_MATH_FAST_HIDE_NULL_OVERFLOW
#undef BINARY_MATH_FAST_HIDE_NULL
#undef UNARY_MATH_EXPENSIVE_HIDE_NULL

}  // namespace noisepage::execution::sql
