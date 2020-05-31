#pragma once

#include <algorithm>
#include <cmath>
#include <stdexcept>

#include "common/common.h"

namespace terrier::execution::sql {

// This file contains a bunch of templated functors that implement many
// Postgres mathematical operators and functions. These operate on raw data
// types, and hence, have no notion of "NULL"-ness. That behaviour is handled at
// a higher-level of the type system, most likely in the functions that call
// into these.
//
// The functors here are sorted alphabetically for convenience.

/**
 * Return the value of the mathematical constant PI.
 */
struct Pi {
  constexpr double operator()() const { return M_PI; }
};

/**
 * Return the value of the mathematical constant E.
 */
struct E {
  constexpr double operator()() const { return M_E; }
};

template <typename T>
struct Abs {
  constexpr T operator()(T input) const { return input < 0 ? -input : input; }
};

template <typename T>
struct Acos {
  constexpr double operator()(T input) const {
    if (input < -1 || input > 1) {
      throw std::runtime_error("ACos is undefined outside [-1,1]");
    }
    return std::acos(input);
  }
};

template <typename T>
struct Asin {
  constexpr double operator()(T input) const {
    if (input < -1 || input > 1) {
      throw std::runtime_error("ASin is undefined outside [-1,1]");
    }
    return std::asin(input);
  }
};

template <typename T>
struct Atan {
  constexpr double operator()(T input) const { return std::atan(input); }
};

template <typename T>
struct Atan2 {
  constexpr double operator()(T a, T b) const { return std::atan2(a, b); }
};

template <typename T>
struct Cbrt {
  constexpr double operator()(T input) const { return std::cbrt(input); }
};

template <typename T>
struct Ceil {
  constexpr T operator()(T input) const { return std::ceil(input); }
};

template <typename T>
struct Cos {
  constexpr double operator()(T input) const { return std::cos(input); }
};

template <typename T>
struct Cosh {
  constexpr double operator()(T input) const { return std::cosh(input); }
};

template <typename T>
struct Cot {
  constexpr double operator()(T input) const { return (1.0 / std::tan(input)); }
};

template <typename T>
struct Degrees {
  constexpr double operator()(T input) const { return input * 180.0 / M_PI; }
};

template <typename T>
struct Exp {
  constexpr double operator()(T input) const { return std::exp(input); }
};

template <typename T>
struct Floor {
  constexpr T operator()(T input) const { return std::floor(input); }
};

template <typename T>
struct Ln {
  constexpr T operator()(T input) const { return std::log(input); }
};

template <typename T>
struct Log {
  constexpr T operator()(T input, T base) const { return std::log(input) / std::log(base); }
};

template <typename T>
struct Log2 {
  constexpr T operator()(T input) const { return std::log2(input); }
};

template <typename T>
struct Log10 {
  constexpr T operator()(T input) const { return std::log10(input); }
};

template <typename T, typename U>
struct Pow {
  constexpr double operator()(T a, U b) { return std::pow(a, b); }
};

template <typename T>
struct Radians {
  constexpr double operator()(T input) const { return input * M_PI / 180.0; }
};

template <typename T>
struct Round {
  constexpr T operator()(T input) const { return input + ((input < 0) ? -0.5 : 0.5); }
};

template <typename T, typename U>
struct RoundUpTo {
  constexpr T operator()(T input, U scale) const {
    if (scale < 0) {
      scale = 0;
    }
    T modifier = std::pow(10U, scale);
    return (static_cast<int64_t>(input * modifier)) / modifier;
  }
};

template <>
struct RoundUpTo<void, void> {
  template <typename T, typename U>
  constexpr T operator()(T input, U scale) const {
    return RoundUpTo<T, U>{}(input, scale);
  }
};

template <typename T>
struct Sign {
  constexpr T operator()(T input) const { return (input > 0) ? 1 : ((input < 0) ? -1.0 : 0); }
};

template <typename T>
struct Sin {
  constexpr double operator()(T input) const { return std::sin(input); }
};

template <typename T>
struct Sinh {
  constexpr double operator()(T input) const { return std::sinh(input); }
};

template <typename T>
struct Sqrt {
  constexpr T operator()(T input) const { return std::sqrt(input); }
};

template <typename T>
struct Tan {
  constexpr double operator()(T input) const { return std::tan(input); }
};

template <typename T>
struct Tanh {
  constexpr double operator()(T input) const { return std::tanh(input); }
};

template <typename T>
struct Truncate {
  constexpr T operator()(T input) const { return std::trunc(input); }
};

}  // namespace terrier::execution::sql
