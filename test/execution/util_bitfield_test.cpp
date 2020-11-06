#include "common/constants.h"
#include "execution/tpl_test.h"
#include "execution/util/bitfield.h"

namespace noisepage::execution::util::test {

TEST(BitfieldTest, SingleElementTest) {
  // Try to encode a single 8-bit character element in a 32-bit integer
  using TestField = BitField32<char, 0, sizeof(char) * common::Constants::K_BITS_PER_BYTE>;

  // First, try a simple test where the character is at position 0
  {
    uint32_t s = TestField::Encode('P');

    EXPECT_EQ('P', TestField::Decode(s));

    s = TestField::Update(s, 'M');

    EXPECT_EQ('M', TestField::Decode(s));
  }

  // Now, try a more complicated scenario where the character is encoded at a
  // non-zero position
  {
    constexpr const char msg[] = "Hello, there!";

    uint32_t s = 0;
    for (uint32_t i = 0; i < sizeof(msg); i++) {
      if (i == 0) {
        s = TestField::Encode(msg[i]);
      } else {
        s = TestField::Update(s, msg[i]);
      }
      EXPECT_EQ(msg[i], TestField::Decode(s));
    }
  }
}

TEST(BitfieldTest, MultiElementTest) {
  // Encode a 16-bit value and an 8-bit value in 32-bit storage
  using U16_BF = BitField32<uint16_t, 0, sizeof(uint16_t) * common::Constants::K_BITS_PER_BYTE>;
  using U8_BF = BitField32<uint8_t, U16_BF::K_NEXT_BIT, sizeof(uint8_t) * common::Constants::K_BITS_PER_BYTE>;

  uint32_t s;

  {
    // First, set the uint16_t field to 1024, check correct

    s = U16_BF::Encode(1024);
    EXPECT_EQ(1024, U16_BF::Decode(s));
  }

  {
    // Now, set the uint8_t field ensuring both uint16_t and uint8_t are set correctly
    s = U8_BF::Update(s, 44);
    EXPECT_EQ(1024, U16_BF::Decode(s));
    EXPECT_EQ(44, U8_BF::Decode(s));
  }

  {
    // Now, update the previous uint8_t field from 44 to 55

    s = U8_BF::Update(s, 55);
    EXPECT_EQ(1024, U16_BF::Decode(s));
    EXPECT_EQ(55, U8_BF::Decode(s));
  }
}

TEST(BitfieldTest, NegativeIntegerTest) {
  // Encode a 16-bit value and an 8-bit value in 32-bit storage
  using U16_BF = BitField32<int16_t, 0, sizeof(uint16_t) * common::Constants::K_BITS_PER_BYTE>;

  auto s = U16_BF::Encode(-16);
  EXPECT_EQ(-16, U16_BF::Decode(s));
}

TEST(BitfieldTest, BooleanElementTest) {
  // Encode a boolean flag at the 32nd bit in a 64-bit bitfield
  using BF = BitField64<bool, 32, 1>;

  // Initial should be set to 0
  uint64_t bitfield = 0;
  EXPECT_FALSE(BF::Decode(bitfield));

  // Now, set the bit
  uint64_t bitfield_2 = BF::Update(bitfield, true);
  EXPECT_TRUE(BF::Decode(bitfield_2));

  // Now reset the bit
  uint64_t bitfield_3 = BF::Update(bitfield_2, false);
  EXPECT_FALSE(BF::Decode(bitfield_3));
}

}  // namespace noisepage::execution::util::test
