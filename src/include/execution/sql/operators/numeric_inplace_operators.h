#pragma once
#include <cmath>

#include "common/macros.h"

namespace terrier::execution::sql {

/**
 * In-place addition.
 */
template <typename T>
struct AddInPlace {
  constexpr void operator()(T *a, T b) const noexcept { *a += b; }
};

/**
 * In-place modulus.
 */
template <typename T>
struct ModuloInPlace {
  constexpr void operator()(T *a, T b) const noexcept {
    // Ensure divisor isn't zero. This should have been checked before here!
    TERRIER_ASSERT(b != 0, "Divide by zero");
    *a %= b;
  }
};

/**
 * Specialization of in-place modulo for floats.
 */
template <>
struct ModuloInPlace<float> {
  void operator()(float *a, float b) const noexcept {
    TERRIER_ASSERT(b != 0, "Divide by zero");
    *a = std::fmod(*a, b);
  }
};

/**
 * Specialization of in-place modulo for double-precision floats.
 */
template <>
struct ModuloInPlace<double> {
  void operator()(double *a, double b) const noexcept {
    TERRIER_ASSERT(b != 0, "Divide by zero");
    *a = std::fmod(*a, b);
  }
};

/**
 * In-place bitwise AND.
 */
template <typename T>
struct BitwiseANDInPlace {
  constexpr void operator()(T *a, T b) const noexcept { *a &= b; }
};

}  // namespace terrier::execution::sql
