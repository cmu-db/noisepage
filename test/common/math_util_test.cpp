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

}  // namespace noisepage::common::test
