#include "common/math_util.h"

#include "test_util/test_harness.h"

namespace noisepage::common::test {

TEST(MathUtilTest, AlignToTest) {
  EXPECT_EQ(2u, MathUtil::AlignTo(1, 2));
  EXPECT_EQ(4u, MathUtil::AlignTo(4, 4));
  EXPECT_EQ(8u, MathUtil::AlignTo(4, 8));
  EXPECT_EQ(8u, MathUtil::AlignTo(8, 8));
  EXPECT_EQ(12u, MathUtil::AlignTo(9, 4));
  EXPECT_EQ(16u, MathUtil::AlignTo(9, 8));
}

namespace {

uint64_t CanonicalFactorialImpl(uint64_t num) {
  return num == 0 || num == 1 ? 1 : num * CanonicalFactorialImpl(num - 1);
}

}  // namespace

TEST(MathUtilTest, Factorial) {
  for (uint32_t i = 0; i < 10; i++) {
    auto ref_result = CanonicalFactorialImpl(i);
    auto actual_result = MathUtil::Factorial(i);
    EXPECT_EQ(ref_result, actual_result) << "!" << i << "=" << ref_result << ", got " << actual_result;
  }
}

namespace {

/** @return The number of leading zeroes in the 128-bit integer @p x. */
int CanonicalNumLeadingZeroes(uint128_t x) {
  // Hacker's Delight [2E Figure 5-19] has a method for computing the number of
  // leading zeroes, but their method is not applicable as we need 128 bits.
  constexpr uint128_t a = (static_cast<uint128_t>(0x0000000000000000) << 64) | 0xFFFFFFFFFFFFFFFF;
  constexpr uint128_t b = (static_cast<uint128_t>(0x00000000FFFFFFFF) << 64) | 0xFFFFFFFFFFFFFFFF;
  constexpr uint128_t c = (static_cast<uint128_t>(0x0000FFFFFFFFFFFF) << 64) | 0xFFFFFFFFFFFFFFFF;
  constexpr uint128_t d = (static_cast<uint128_t>(0x00FFFFFFFFFFFFFF) << 64) | 0xFFFFFFFFFFFFFFFF;
  constexpr uint128_t e = (static_cast<uint128_t>(0x0FFFFFFFFFFFFFFF) << 64) | 0xFFFFFFFFFFFFFFFF;
  constexpr uint128_t f = (static_cast<uint128_t>(0x3FFFFFFFFFFFFFFF) << 64) | 0xFFFFFFFFFFFFFFFF;
  constexpr uint128_t g = (static_cast<uint128_t>(0x7FFFFFFFFFFFFFFF) << 64) | 0xFFFFFFFFFFFFFFFF;

  if (x == 0) return (128);

  int n = 0;
  if (x <= a) {
    n = n + 64;
    x = x << 64;
  }
  if (x <= b) {
    n = n + 32;
    x = x << 32;
  }
  if (x <= c) {
    n = n + 16;
    x = x << 16;
  }
  if (x <= d) {
    n = n + 8;
    x = x << 8;
  }
  if (x <= e) {
    n = n + 4;
    x = x << 4;
  }
  if (x <= f) {
    n = n + 2;
    x = x << 2;
  }
  if (x <= g) {
    n = n + 1;
  }
  return n;
}

}  // namespace

TEST(MathUtilTest, NumLeadingZeroes) {
  for (uint128_t x : {1, 324325}) {
    for (int i = 0; i < 127; i++) {
      EXPECT_EQ(CanonicalNumLeadingZeroes(x), MathUtil::GetNumLeadingZeroesAssumingNonZero(x));
      x = x << 1;
    }
  }
}

}  // namespace noisepage::common::test
