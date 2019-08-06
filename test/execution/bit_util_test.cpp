#include "execution/tpl_test.h"  // NOLINT

#include "execution/util/bit_util.h"

namespace terrier::util::test {

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

  BitVector bv(100);
  for (u32 i = 0; i < bv.num_bits(); i++) {
    EXPECT_FALSE(bv[i]);
  }
}

// NOLINTNEXTLINE
TEST(BitUtilTest, ClearBits) {
  //
  // Create a bit vector, set all the bits, clear it, check
  //

  BitVector bv(10);
  for (u32 i = 0; i < bv.num_bits(); i++) {
    bv.Set(i);
  }
  bv.ClearAll();
  for (u32 i = 0; i < bv.num_bits(); i++) {
    EXPECT_FALSE(bv[i]);
  }
}

// NOLINTNEXTLINE
TEST(BitUtilTest, TestAndSet) {
  //
  // Create a BitVector, set every odd bit position
  //

  BitVector bv(100);
  for (u32 i = 0; i < bv.num_bits(); i++) {
    if (i % 2 == 0) {
      bv.Set(i);
    }
  }

  for (u32 i = 0; i < bv.num_bits(); i++) {
    if (i % 2 == 0) {
      EXPECT_TRUE(bv[i]);
    } else {
      EXPECT_FALSE(bv[i]);
    }
  }

  // Flip every bit
  for (u32 i = 0; i < bv.num_bits(); i++) {
    bv.Flip(i);
  }

  // Now, every even bit position should be set
  for (u32 i = 0; i < bv.num_bits(); i++) {
    if (i % 2 == 0) {
      EXPECT_FALSE(bv[i]);
    } else {
      EXPECT_TRUE(bv[i]);
    }
  }

  // Now, manually unset every bit
  for (u32 i = 0; i < bv.num_bits(); i++) {
    bv.Unset(i);
  }

  // Ensure all unset
  for (u32 i = 0; i < bv.num_bits(); i++) {
    EXPECT_FALSE(bv[i]);
  }
}

// NOLINTNEXTLINE
TEST(BitUtilTest, SetToValue) {
  BitVector bv(100);

  bv.ClearAll();
  bv.SetTo(10, true);

  for (u32 i = 0; i < bv.num_bits(); i++) {
    EXPECT_EQ(i == 10u, bv.Test(i));
  }

  bv.SetTo(80, true);
  bv.SetTo(10, false);

  for (u32 i = 0; i < bv.num_bits(); i++) {
    EXPECT_EQ(i == 80u, bv.Test(i));
  }

  bv.SetTo(80, false);

  for (u32 i = 0; i < bv.num_bits(); i++) {
    EXPECT_FALSE(bv.Test(i));
  }
}

// NOLINTNEXTLINE
TEST(BitUtilTest, InlinedBitVector) {
  InlinedBitVector<64> bits;

  EXPECT_EQ(64u, bits.num_bits());

  // Initially all false
  for (u32 i = 0; i < bits.num_bits(); i++) {
    EXPECT_FALSE(bits.Test(i));
  }

  // Set even bits
  for (u32 i = 0; i < bits.num_bits(); i++) {
    if (i % 2 == 0) {
      bits.Set(i);
    }
  }

  // Check
  for (u32 i = 0; i < bits.num_bits(); i++) {
    auto set = bits.Test(i);
    if (i % 2 == 0) {
      EXPECT_TRUE(set);
    } else {
      EXPECT_FALSE(set);
    }
  }

  // Clear
  bits.ClearAll();

  // Final check all 0
  for (u32 i = 0; i < bits.num_bits(); i++) {
    EXPECT_FALSE(bits.Test(i));
  }
}

}  // namespace terrier::util::test
