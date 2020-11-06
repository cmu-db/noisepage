#include "execution/util/bit_util.h"

#include "execution/tpl_test.h"

namespace noisepage::execution::util::test {

// NOLINTNEXTLINE
TEST(BitUtilTest, BitVectorSize) {
  // We need at least one word for 1 bit
  EXPECT_EQ(1u, BitUtil::Num32BitWordsFor(1));

  // We still need one 32-bit word for 31 and 32 bits
  EXPECT_EQ(1u, BitUtil::Num32BitWordsFor(31));
  EXPECT_EQ(1u, BitUtil::Num32BitWordsFor(32));

  // For 33 elements, we need two 32-bit words
  EXPECT_EQ(2u, BitUtil::Num32BitWordsFor(33));
}

// NOLINTNEXTLINE
TEST(BitUtilTest, EmptyBitVector) {
  //
  // Create an empty bit vector, ensure all bits unset
  //

  uint32_t num_bits = 100;
  uint32_t bv[BitUtil::Num32BitWordsFor(num_bits)];
  BitUtil::Clear(bv, num_bits);
  for (uint32_t i = 0; i < num_bits; i++) {
    EXPECT_FALSE(BitUtil::Test(bv, i));
  }
}

// NOLINTNEXTLINE
TEST(BitUtilTest, ClearBits) {
  //
  // Create a bit vector, set all the bits, clear it, check
  //

  uint32_t num_bits = 10;
  uint32_t bv[BitUtil::Num32BitWordsFor(num_bits)];
  BitUtil::Clear(bv, num_bits);
  for (uint32_t i = 0; i < num_bits; i++) {
    BitUtil::Set(bv, i);
  }
  BitUtil::Clear(bv, num_bits);
  for (uint32_t i = 0; i < num_bits; i++) {
    EXPECT_FALSE(BitUtil::Test(bv, i));
  }
}

// NOLINTNEXTLINE
TEST(BitUtilTest, TestAndSet) {
  //
  // Create a BitVector, set every odd bit position
  //

  uint32_t num_bits = 100;
  uint32_t bv[BitUtil::Num32BitWordsFor(num_bits)];
  BitUtil::Clear(bv, num_bits);
  for (uint32_t i = 0; i < num_bits; i++) {
    if (i % 2 == 0) {
      BitUtil::Set(bv, i);
    }
  }

  for (uint32_t i = 0; i < num_bits; i++) {
    if (i % 2 == 0) {
      EXPECT_TRUE(BitUtil::Test(bv, i));
    } else {
      EXPECT_FALSE(BitUtil::Test(bv, i));
    }
  }

  // Flip every bit
  for (uint32_t i = 0; i < num_bits; i++) {
    BitUtil::Flip(bv, i);
  }

  // Now, every even bit position should be set
  for (uint32_t i = 0; i < num_bits; i++) {
    if (i % 2 == 0) {
      EXPECT_FALSE(BitUtil::Test(bv, i));
    } else {
      EXPECT_TRUE(BitUtil::Test(bv, i));
    }
  }

  // Now, manually unset every bit
  for (uint32_t i = 0; i < num_bits; i++) {
    BitUtil::Unset(bv, i);
  }

  // Ensure all unset
  for (uint32_t i = 0; i < num_bits; i++) {
    EXPECT_FALSE(BitUtil::Test(bv, i));
  }
}

// NOLINTNEXTLINE
TEST(BitUtilTest, SetToValue) {
  uint32_t num_bits = 100;
  uint32_t bv[BitUtil::Num32BitWordsFor(num_bits)];
  BitUtil::Clear(bv, num_bits);

  BitUtil::SetTo(bv, 10, true);

  for (uint32_t i = 0; i < num_bits; i++) {
    EXPECT_EQ(i == 10u, BitUtil::Test(bv, i));
  }

  BitUtil::SetTo(bv, 80, true);
  BitUtil::SetTo(bv, 10, false);

  for (uint32_t i = 0; i < num_bits; i++) {
    EXPECT_EQ(i == 80u, BitUtil::Test(bv, i));
  }

  BitUtil::SetTo(bv, 80, false);

  for (uint32_t i = 0; i < num_bits; i++) {
    EXPECT_FALSE(BitUtil::Test(bv, i));
  }
}

}  // namespace noisepage::execution::util::test
