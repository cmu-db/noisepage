/*
 * A C++ implementation of Chris Doty-Humphrey's SFC PRNG(s)
 *
 * The MIT License (MIT)
 *
 * Copyright (c) 2018 Melissa E. O'Neill
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 */

/*
 * Based on code written by Chris Doty-Humphrey, adapted for C++-style
 * random-number generation.
 */

/*
 * Modified for TPL.
 */

#pragma once

namespace noisepage {

/** The number of bits per byte */
static constexpr const uint32_t kBitsPerByte = 8;

/** The default vector size to use when performing vectorized iteration */
static constexpr const uint32_t kDefaultVectorSize = 2048;

/** The default prefetch distance to use */
static constexpr const uint32_t kPrefetchDistance = 16;

namespace sfc_detail {

/**
 * A fast implementation of Chris Doty-Humphrey's SFC PRNG(s). Serves as a drop-in replacement for
 * any of the STL's PRNGs. These are MORE random and FASTER to generate. These tend to be so fast as
 * to shift the bottleneck from random generation to generation-within-bounds. This generator is
 * ~2-3x faster than STL's fastest generator (i.e., Mersenne Twister), but if you use it with one of
 * the STL's distributions (i.e., std::uniform_int_distribution<>), you won't reap the full benefit.
 *
 * @tparam InputType The numeric input type.
 * @tparam ReturnType The numeric output type.
 * @tparam p The 'p' parameter to the algorithm.
 * @tparam q The 'q' parameter to the algorithm.
 * @tparam r The 'r' parameter to the algorithm.
 */
template <typename InputType, typename ReturnType, uint32_t p, uint32_t q, uint32_t r>
class SFC {
 private:
  // Size (in bits) of input and output type.
  static constexpr uint32_t kBitsInInputType = kBitsPerByte * sizeof(InputType);
  static constexpr uint32_t kBitsInOutputType = kBitsPerByte * sizeof(ReturnType);

  // Rotate the given input value left.
  static constexpr InputType RotateLeft(InputType x, uint32_t k) noexcept {
    return (x << k) | (x >> (kBitsInInputType - k));
  }

 public:
  using result_type = ReturnType;
  using state_type = InputType;

  /**
   * Create a new SFC generator with the given input seed.
   * @param seed The seed to use.
   */
  SFC(InputType seed = InputType(0xcafef00dbeef5eedULL)) : SFC(seed, seed, seed) {}

  /**
   * Create a new SFC generator with the given three-part seed.
   * @param seed1 The first seed component.
   * @param seed2 The second seed component.
   * @param seed3 The third seed component.
   */
  SFC(InputType seed1, InputType seed2, InputType seed3)
      : a_(seed3), b_(seed2), c_(seed1), d_(InputType(1)) {
    for (uint32_t i = 0; i < 12; i++) {
      Advance();
    }
  }

  /**
   * Advance the generator by one element.
   */
  void Advance() noexcept { (void)operator()(); }

  /**
   * @return The minimum possible value returned by the generator. Needed by STL.
   */
  static constexpr result_type min() { return 0; }

  /**
   * @return The maximum possible value returned by the generator. Needed by STL.
   */
  static constexpr result_type max() { return ~result_type(0); }

  /**
   * @return The next random number.
   */
  ReturnType operator()() noexcept {
    InputType tmp = a_ + b_ + d_++;
    a_ = b_ ^ (b_ >> q);
    b_ = c_ + (c_ << r);
    c_ = RotateLeft(c_, p) + tmp;
    return ReturnType(tmp);
  }

  /**
   * @return True if this generator is equivalent to the provided generator; false otherwise.
   */
  bool operator==(const SFC &rhs) const noexcept {
    return (a_ == rhs.a_) && (b_ == rhs.b_) && (c_ == rhs.c_) && (d_ == rhs.d_);
  }

  /**
   * @return True if this generator is not equal to the provided generator; false otherwise.
   */
  bool operator!=(const SFC &rhs) const noexcept { return !operator==(rhs); }

 private:
  InputType a_, b_, c_, d_;  // State components.
};

}  // namespace sfc_detail

// - 256 state bits, uint64_t output

using SFC64a = sfc_detail::SFC<uint64_t, uint64_t, 24, 11, 3>;
using SFC64b = sfc_detail::SFC<uint64_t, uint64_t, 25, 12, 3>;  // old, less good

using SFC64 = SFC64a;

// - 128 state bits, uint32_t output

using SFC32a = sfc_detail::SFC<uint32_t, uint32_t, 21, 9, 3>;
using SFC32b = sfc_detail::SFC<uint32_t, uint32_t, 15, 8, 3>;
using SFC32c = sfc_detail::SFC<uint32_t, uint32_t, 25, 8, 3>;  // old, less good

using SFC32 = SFC32a;

// TINY VERSIONS FOR TESTING AND SPECIALIZED USES ONLY

// - 64 state bits, uint16_t output

using SFC16a = sfc_detail::SFC<uint16_t, uint16_t, 4, 3, 2>;
using SFC16b = sfc_detail::SFC<uint16_t, uint16_t, 6, 5, 2>;
using SFC16c = sfc_detail::SFC<uint16_t, uint16_t, 4, 5, 3>;
using SFC16d = sfc_detail::SFC<uint16_t, uint16_t, 6, 5, 3>;
using SFC16e = sfc_detail::SFC<uint16_t, uint16_t, 7, 5, 3>;
using SFC16f = sfc_detail::SFC<uint16_t, uint16_t, 7, 3, 2>;  // old, less good

using SFC16 = SFC16d;

// - 32 state bits, uint8_t output
// Not by Chris

using SFC8 = sfc_detail::SFC<uint8_t, uint8_t, 3, 2, 1>;

}  // namespace noisepage