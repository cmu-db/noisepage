#pragma once
#include "common/macros.h"
#include "execution/util/execution_common.h"

namespace noisepage::execution::util::simd {

/**
 * Stores the width of a lane
 */
struct Bitwidth {
  /**
   * Width of a lane
   */
  static constexpr const uint32_t
#if defined(__AVX512F__)
      VALUE = 512;
#elif defined(__AVX2__)
      VALUE = 256;
#else
      VALUE = 256;
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
  static constexpr const uint32_t COUNT = Bitwidth::VALUE / (sizeof(T) * 8);
};

}  // namespace noisepage::execution::util::simd

#define SIMD_TOP_LEVEL

#if defined(__AVX512F__)
#include "execution/util/simd/avx512.h"  // NOLINT
#elif defined(__AVX2__)
#include "execution/util/simd/avx2.h"  // NOLINT
#endif

#undef SIMD_TOP_LEVEL
