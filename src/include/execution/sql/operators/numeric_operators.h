#pragma once

#include <algorithm>
#include <cmath>
#include <stdexcept>

#include "common/error/error_code.h"
#include "common/error/exception.h"

namespace noisepage::execution::sql {

// This file contains a bunch of templated functors that implement many
// Postgres mathematical operators and functions. These operate on raw data
// types, and hence, have no notion of "NULL"-ness. That behaviour is handled at
// a higher-level of the type system, most likely in the functions that call
// into these.
//
// The functors here are sorted alphabetically for convenience.

/** Return the value of the mathematical constant PI. */
struct Pi {
  /** @return Pi. */
  constexpr double operator()() const { return M_PI; }
};

/** Return the value of the mathematical constant E. */
struct E {
  /** @return E. */
  constexpr double operator()() const { return M_E; }
};

/** Compute the function Abs. */
template <typename T>
struct Abs {
  /** @return Abs(input). */
  constexpr T operator()(T input) const { return input < 0 ? -input : input; }
};

/** Compute the function Acos. */
template <typename T>
struct Acos {
  /** @return Acos(input). */
  constexpr double operator()(T input) const {
    if (input < -1 || input > 1) {
      throw EXECUTION_EXCEPTION("ACos is undefined outside [-1,1]",
                                common::ErrorCode::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
    }
    return std::acos(input);
  }
};

/** Compute the function Asin. */
template <typename T>
struct Asin {
  /** @return Asin(input). */
  constexpr double operator()(T input) const {
    if (input < -1 || input > 1) {
      throw EXECUTION_EXCEPTION("ASin is undefined outside [-1,1]",
                                common::ErrorCode::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
    }
    return std::asin(input);
  }
};

/** Compute the function Atan. */
template <typename T>
struct Atan {
  /** @return Atan(input). */
  constexpr double operator()(T input) const { return std::atan(input); }
};

/** Compute the function Atan2. */
template <typename T>
struct Atan2 {
  /** @return Atan2(input). */
  constexpr double operator()(T a, T b) const { return std::atan2(a, b); }
};

/** Compute the function Cbrt. */
template <typename T>
struct Cbrt {
  /** @return Cbrt(input). */
  constexpr double operator()(T input) const { return std::cbrt(input); }
};

/** Compute the function Ceil. */
template <typename T>
struct Ceil {
  /** @return Ceil(input). */
  constexpr T operator()(T input) const { return std::ceil(input); }
};

/** Compute the function Cos. */
template <typename T>
struct Cos {
  /** @return Cos(input). */
  constexpr double operator()(T input) const { return std::cos(input); }
};

/** Compute the function Cosh. */
template <typename T>
struct Cosh {
  /** @return Cosh(input). */
  constexpr double operator()(T input) const { return std::cosh(input); }
};

/** Compute the function Cot. */
template <typename T>
struct Cot {
  /** @return Cot(input). */
  constexpr double operator()(T input) const { return (1.0 / std::tan(input)); }
};

/** Compute the function Degrees. */
template <typename T>
struct Degrees {
  /** @return Degrees(input). */
  constexpr double operator()(T input) const { return input * 180.0 / M_PI; }
};

/** Compute the function Exp. */
template <typename T>
struct Exp {
  /** @return Exp(input). */
  constexpr double operator()(T input) const { return std::exp(input); }
};

/** Compute the function Floor. */
template <typename T>
struct Floor {
  /** @return Floor(input). */
  constexpr T operator()(T input) const { return std::floor(input); }
};

/** Compute the function Ln. */
template <typename T>
struct Ln {
  /** @return Ln(input). */
  constexpr T operator()(T input) const { return std::log(input); }
};

/** Compute the function Log. */
template <typename T>
struct Log {
  /** @return Log(input). */
  constexpr T operator()(T input, T base) const { return std::log(input) / std::log(base); }
};

/** Compute the function Log2. */
template <typename T>
struct Log2 {
  /** @return Log2(input). */
  constexpr T operator()(T input) const { return std::log2(input); }
};

/** Compute the function Log10. */
template <typename T>
struct Log10 {
  /** @return Log10(input). */
  constexpr T operator()(T input) const { return std::log10(input); }
};

/** Compute the function Pow. */
template <typename T, typename U>
struct Pow {
  /** @return Pow(input). */
  constexpr double operator()(T a, U b) { return std::pow(a, b); }
};

/** Compute the function Radians. */
template <typename T>
struct Radians {
  /** @return Radians(input). */
  constexpr double operator()(T input) const { return input * M_PI / 180.0; }
};

/** Compute the function Round. */
template <typename T>
struct Round {
  /** @return Round(input). */
  constexpr T operator()(T input) const { return std::round(input); }
};

/** Compute the function RoundUpTo. */
template <typename T, typename U>
struct RoundUpTo {
  /** @return RoundUpTo(input). */
  constexpr T operator()(T input, U scale) const {
    if (scale < 0) {
      scale = 0;
    }
    T modifier = std::pow(10U, scale);
    return (std::round(input * modifier)) / modifier;
  }
};

/** Compute the function RoundUpTo. */
template <>
struct RoundUpTo<void, void> {
  /** @return RoundUpTo(input). */
  template <typename T, typename U>
  constexpr T operator()(T input, U scale) const {
    return RoundUpTo<T, U>{}(input, scale);
  }
};

/** Compute the function Sign. */
template <typename T>
struct Sign {
  /** @return Sign(input). */
  constexpr T operator()(T input) const { return (input > 0) ? 1 : ((input < 0) ? -1.0 : 0); }
};

/** Compute the function Sin. */
template <typename T>
struct Sin {
  /** @return Sin(input). */
  constexpr double operator()(T input) const { return std::sin(input); }
};

/** Compute the function Sinh. */
template <typename T>
struct Sinh {
  /** @return Sinh(input). */
  constexpr double operator()(T input) const { return std::sinh(input); }
};

/** Compute the function Sqrt. */
template <typename T>
struct Sqrt {
  /** @return Sqrt(input). */
  constexpr T operator()(T input) const { return std::sqrt(input); }
};

/** Compute the function Tan. */
template <typename T>
struct Tan {
  /** @return Tan(input). */
  constexpr double operator()(T input) const { return std::tan(input); }
};

/** Compute the function Tanh. */
template <typename T>
struct Tanh {
  /** @return Tanh(input). */
  constexpr double operator()(T input) const { return std::tanh(input); }
};

/** Compute the function Truncate. */
template <typename T>
struct Truncate {
  /** @return Truncate(input). */
  constexpr T operator()(T input) const { return std::trunc(input); }
};

}  // namespace noisepage::execution::sql
