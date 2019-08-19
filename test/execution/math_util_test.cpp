#include "execution/tpl_test.h"

#include "common/math_util.h"

namespace terrier::execution::util::test {

// NOLINTNEXTLINE
TEST(MathUtilTest, AlignToTest) {
  EXPECT_EQ(2u, common::MathUtil::AlignTo(1, 2));
  EXPECT_EQ(4u, common::MathUtil::AlignTo(4, 4));
  EXPECT_EQ(8u, common::MathUtil::AlignTo(4, 8));
  EXPECT_EQ(8u, common::MathUtil::AlignTo(8, 8));
  EXPECT_EQ(12u, common::MathUtil::AlignTo(9, 4));
  EXPECT_EQ(16u, common::MathUtil::AlignTo(9, 8));
}

}  // namespace terrier::execution::util::test
