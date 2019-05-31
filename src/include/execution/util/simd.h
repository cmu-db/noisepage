#pragma once

#include "execution/util/common.h"
#include "execution/util/macros.h"

namespace tpl::util::simd {

/**
 * Stores the width of a lane
 */
struct Bitwidth {
  /**
   * Width of a lane
   */
  static constexpr const u32
#if defined(__AVX512F__)
      value = 512;
#elif defined(__AVX2__)
      value = 256;
#else
      value = 256;
#endif
};

/**
 * A simd lane
 * @tparam T: type of individual elements
 */
template <typename T>
struct Lane {
  /**
   * Number of elements in the SIMD lane.
   */
  static constexpr const u32 count = Bitwidth::value / (sizeof(T) * 8);
};

}  // namespace tpl::util::simd

#define SIMD_TOP_LEVEL

#if defined(__AVX512F__)
#include "execution/util/simd/avx512.h"  // NOLINT
#elif defined(__AVX2__)
#include "execution/util/simd/avx2.h"  // NOLINT
#else
#error "Compiler must support at least AVX2"
#endif

#undef SIMD_TOP_LEVEL
