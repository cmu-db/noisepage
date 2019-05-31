#include "execution/tpl_test.h"  // NOLINT

#include "execution/util/bitfield.h"

namespace tpl::util::test {

// NOLINTNEXTLINE
TEST(BitfieldTest, SingleElementTest) {
  // Try to encode a single 8-bit character element in a 32-bit integer
  using TestField = BitField32<char, 0, sizeof(char) * kBitsPerByte>;

  // First, try a simple test where the character is at position 0
  {
    u32 s = TestField::Encode('P');

    EXPECT_EQ('P', TestField::Decode(s));

    s = TestField::Update(s, 'M');

    EXPECT_EQ('M', TestField::Decode(s));
  }

  // Now, try a more complicated scenario where the character is encoded at a
  // non-zero position
  {
    constexpr const char msg[] = "Hello, there!";

    u32 s = 0;
    for (u32 i = 0; i < sizeof(msg); i++) {
      if (i == 0) {
        s = TestField::Encode(msg[i]);
      } else {
        s = TestField::Update(s, msg[i]);
      }
      EXPECT_EQ(msg[i], TestField::Decode(s));
    }
  }
}

// NOLINTNEXTLINE
TEST(BitfieldTest, MultiElementTest) {
  // Encode a 16-bit value and an 8-bit value in 32-bit storage
  using U16_BF = BitField32<u16, 0, sizeof(u16) * kBitsPerByte>;
  using U8_BF = BitField32<u8, U16_BF::kNextBit, sizeof(u8) * kBitsPerByte>;

  u32 s;

  {
    // First, set the u16 field to 1024, check correct

    s = U16_BF::Encode(1024);
    EXPECT_EQ(1024, U16_BF::Decode(s));
  }

  {
    // Now, set the u8 field ensuring both u16 and u8 are set correctly
    s = U8_BF::Update(s, 44);
    EXPECT_EQ(1024, U16_BF::Decode(s));
    EXPECT_EQ(44, U8_BF::Decode(s));
  }

  {
    // Now, update the previous u8 field from 44 to 55

    s = U8_BF::Update(s, 55);
    EXPECT_EQ(1024, U16_BF::Decode(s));
    EXPECT_EQ(55, U8_BF::Decode(s));
  }
}

// NOLINTNEXTLINE
TEST(BitfieldTest, NegativeIntegerTest) {
  // Encode a 16-bit value and an 8-bit value in 32-bit storage
  using U16_BF = BitField32<i16, 0, sizeof(u16) * kBitsPerByte>;

  auto s = U16_BF::Encode(-16);
  EXPECT_EQ(-16, U16_BF::Decode(s));
}

// NOLINTNEXTLINE
TEST(BitfieldTest, BooleanElementTest) {
  // Encode a boolean flag at the 32nd bit in a 64-bit bitfield
  using BF = BitField64<bool, 32, 1>;

  // Initial should be set to 0
  u64 bitfield = 0;
  EXPECT_FALSE(BF::Decode(bitfield));

  // Now, set the bit
  u64 bitfield_2 = BF::Update(bitfield, true);
  EXPECT_TRUE(BF::Decode(bitfield_2));

  // Now reset the bit
  u64 bitfield_3 = BF::Update(bitfield_2, false);
  EXPECT_FALSE(BF::Decode(bitfield_3));
}

}  // namespace tpl::util::test
