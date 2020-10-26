#pragma once
#include <cmath>

#include "common/macros.h"

namespace noisepage::execution::sql {

/** In-place addition. */
template <typename T>
struct AddInPlace {
  /** Perform a += b. */
  constexpr void operator()(T *a, T b) const noexcept { *a += b; }
};

/** In-place modulus. */
template <typename T>
struct ModuloInPlace {
  /** Perform a %= b. */
  constexpr void operator()(T *a, T b) const noexcept {
    // Ensure divisor isn't zero. This should have been checked before here!
    NOISEPAGE_ASSERT(b != 0, "Divide by zero");
    *a %= b;
  }
};

/** Specialization of in-place modulo for floats. */
template <>
struct ModuloInPlace<float> {
  /** Perform a %= b. */
  void operator()(float *a, float b) const noexcept {
    NOISEPAGE_ASSERT(b != 0, "Divide by zero");
    *a = std::fmod(*a, b);
  }
};

/** Specialization of in-place modulo for double-precision floats. */
template <>
struct ModuloInPlace<double> {
  /** Perform a %= b. */
  void operator()(double *a, double b) const noexcept {
    NOISEPAGE_ASSERT(b != 0, "Divide by zero");
    *a = std::fmod(*a, b);
  }
};

/** In-place bitwise AND. */
template <typename T>
struct BitwiseANDInPlace {
  /** Perform a &= b. */
  constexpr void operator()(T *a, T b) const noexcept { *a &= b; }
};

}  // namespace noisepage::execution::sql
