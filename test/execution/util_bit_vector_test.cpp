#include <random>
#include <unordered_set>
#include <vector>

#include "execution/tpl_test.h"
#include "execution/util/bit_vector.h"

namespace terrier::execution::util::test {

namespace {

// Verify that the callback returns true for all set bit indexes
template <typename BitVectorType, typename F>
::testing::AssertionResult Verify(BitVectorType &bv, F &&f) {  // NOLINT
  for (uint32_t i = 0; i < bv.GetNumBits(); i++) {
    if (bv[i] && !f(i)) {
      return ::testing::AssertionFailure() << "bv[" << i << "]=true, but expected to be false";
    }
  }
  return ::testing::AssertionSuccess();
}

/**
 * Verify that only bits at indexes contained in @em idxs are set within the bit vector @em bv.
 * @tparam BitVectorType The type of bit vector.
 * @param bv The input bit vector to check.
 * @param idxs The indexes of the positions in the bit vector where '1's are expected.
 * @return True if only specific bits are set; false otherwise.
 */
template <typename BitVectorType>
::testing::AssertionResult Verify(const BitVectorType &bv, std::initializer_list<uint32_t> idxs) {
  std::unordered_set<uint32_t> positions(idxs);

  uint32_t num_set = 0;
  auto success = Verify(bv, [&](auto idx) {
    num_set++;
    return positions.count(idx) != 0;
  });

  if (!success) {
    return success;
  }

  if (num_set != positions.size()) {
    return ::testing::AssertionFailure() << "Unmatched # set bits. Actual: " << num_set
                                         << ", Expected: " << positions.size();
  }

  return ::testing::AssertionSuccess();
}

}  // namespace

BitVector<> Make(std::initializer_list<uint32_t> vals) {
  BitVector<> bv(vals.size());
  std::for_each(vals.begin(), vals.end(), [&, i = 0](auto &bval) mutable { bv.Set(i++, bval); });
  return bv;
}

TEST(BitVectorTest, BitVectorSize) {
  // We need at least one word for 1 bit
  EXPECT_EQ(1u, BitVector<>::NumNeededWords(1));

  // We still need one 64-bit word for 63 and 64 bits
  EXPECT_EQ(1u, BitVector<>::NumNeededWords(63));
  EXPECT_EQ(1u, BitVector<>::NumNeededWords(64));

  // For 33 elements, we need two 32-bit words
  EXPECT_EQ(2u, BitVector<>::NumNeededWords(65));
}

TEST(BitVectorTest, Init) {
  BitVector<> bv(100);
  EXPECT_EQ(2u, bv.GetNumWords());
  EXPECT_EQ(100u, bv.GetNumBits());

  for (uint32_t i = 0; i < bv.GetNumBits(); i++) {
    EXPECT_FALSE(bv[i]);
  }
}

TEST(BitVectorTest, Set) {
  BitVector<> bv(10);

  bv.Set(2);
  EXPECT_TRUE(Verify(bv, {2}));

  bv.Set(0);
  EXPECT_TRUE(Verify(bv, {0, 2}));

  bv.Set(7);
  EXPECT_TRUE(Verify(bv, {0, 2, 7}));
}

TEST(BitVectorTest, IndexOperator) {
  BitVector<> bv(10);

  bv[2] = true;
  EXPECT_TRUE(Verify(bv, {2}));

  bv[0] = true;
  EXPECT_TRUE(Verify(bv, {0, 2}));

  bv[7] = true;
  EXPECT_TRUE(Verify(bv, {0, 2, 7}));

  bv[0] = bv[7] = false;
  EXPECT_TRUE(Verify(bv, {2}));

  bv.Reset();
  bv[8] = true;
  bv[9] = bv[8];
  EXPECT_TRUE(Verify(bv, {8, 9}));
}

TEST(BitVectorTest, SetAll) {
  BitVector<> bv(300);

  bv.Set(2);
  EXPECT_TRUE(Verify(bv, {2}));

  bv.Set(299);
  EXPECT_TRUE(Verify(bv, {2, 299}));

  bv.SetAll();
  for (uint32_t i = 0; i < bv.GetNumBits(); i++) {
    EXPECT_TRUE(bv[i]);
  }
}

TEST(BitVectorTest, SetRange) {
  BitVector<> bv(300);

  // Check if lo <= v < hi, if v is in the range [lo, hi)
  const auto in_range = [](uint32_t v, uint32_t lo, uint32_t hi) { return lo <= v && v < hi; };

  // No change
  bv.SetRange(0, 0);
  EXPECT_TRUE(Verify(bv, {}));

  // Only bv[0] is set
  bv.SetRange(0, 1);
  EXPECT_TRUE(Verify(bv, {0}));

  // Try larger within-word change
  bv.Reset();
  bv.SetRange(27, 57);
  EXPECT_TRUE(Verify(bv, [&](auto idx) { return in_range(idx, 27, 57); }));

  // Try cross-word change with end at boundary
  bv.Reset();
  bv.SetRange(10, 64);
  EXPECT_TRUE(Verify(bv, [&](auto idx) { return in_range(idx, 10, 64); }));

  // Try multi-word change
  bv.Reset();
  bv.SetRange(60, 300);
  EXPECT_TRUE(Verify(bv, [&](auto idx) { return in_range(idx, 60, 300); }));
}

TEST(BitVectorTest, SetTo) {
  BitVector<> bv(10);

  bv.Set(2, true);
  EXPECT_TRUE(Verify(bv, {2}));

  // Repeats work
  bv.Set(2, true);
  EXPECT_TRUE(Verify(bv, {2}));

  bv.Set(2, false);
  EXPECT_TRUE(Verify(bv, {}));

  bv.Set(3, true);
  EXPECT_TRUE(Verify(bv, {3}));

  bv.Set(2, true);
  EXPECT_TRUE(Verify(bv, {2, 3}));

  bv.SetAll();
  EXPECT_TRUE(Verify(bv, {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}));
}

TEST(BitVectorTest, SetWord) {
  BitVector<> bv(10);

  // bv[0] = true
  bv.SetWord(0, 0x00000001);

  EXPECT_TRUE(bv.Test(0));
  for (uint32_t i = 1; i < bv.GetNumBits(); i++) {
    EXPECT_FALSE(bv.Test(i));
  }

  // bv[9] = true
  bv.SetWord(0, 0x00000200);
  EXPECT_TRUE(bv.Test(9));
  for (uint32_t i = 0; i < bv.GetNumBits() - 1; i++) {
    EXPECT_FALSE(bv.Test(i));
  }

  // Try setting out of bound bits
  bv.SetWord(0, 0xfffff800);
  EXPECT_FALSE(bv.Any());

  // Even bits only
  bv.SetWord(0, 0x55555555);
  for (uint32_t i = 0; i < bv.GetNumBits(); i++) {
    EXPECT_EQ(i % 2 == 0, bv.Test(i));
  }
}

TEST(BitVectorTest, Unset) {
  BitVector<> bv(10);

  // Set every 3rd bit
  for (uint32_t i = 0; i < bv.GetNumBits(); i++) {
    if (i % 3 == 0) {
      bv.Set(i);
    }
  }
  EXPECT_TRUE(Verify(bv, {0, 3, 6, 9}));

  bv.Unset(3);
  EXPECT_TRUE(Verify(bv, {0, 6, 9}));

  bv.Unset(9);
  EXPECT_TRUE(Verify(bv, {0, 6}));

  bv.Reset();
  EXPECT_TRUE(Verify(bv, {}));
}

TEST(BitVectorTest, TestAndSet) {
  // Simple set
  {
    BitVector<> bv(100);
    EXPECT_FALSE(bv.Test(20));
    bv.Set(20);
    EXPECT_TRUE(bv.Test(20));
  }

  // Set even bits only, then check
  {
    BitVector<> bv(100);
    for (uint32_t i = 0; i < bv.GetNumBits(); i++) {
      if (i % 2 == 0) {
        bv.Set(i);
      }
    }
    for (uint32_t i = 0; i < bv.GetNumBits(); i++) {
      EXPECT_EQ(i % 2 == 0, bv[i]);
    }
  }
}

TEST(BitVectorTest, Flip) {
  BitVector<> bv(10);

  // Set even bits
  for (uint32_t i = 0; i < bv.GetNumBits(); i++) {
    if (i % 2 == 0) {
      bv.Set(i);
    }
  }
  EXPECT_TRUE(Verify(bv, {0, 2, 4, 6, 8}));

  bv.Flip(0);
  EXPECT_TRUE(Verify(bv, {2, 4, 6, 8}));

  bv.Flip(8);
  EXPECT_TRUE(Verify(bv, {2, 4, 6}));

  bv.FlipAll();
  EXPECT_TRUE(Verify(bv, {0, 1, 3, 5, 7, 8, 9}));
}

TEST(BitVectorTest, FlipAll) {
  BitVector<> bv(257);

  bv.Set(256);
  EXPECT_TRUE(bv[256]);
  bv.FlipAll();
  EXPECT_FALSE(bv[256]);

  // Set even bits
  bv.Reset();
  for (uint32_t i = 0; i < bv.GetNumBits(); i++) {
    if (i % 2 == 0) {
      bv.Set(i);
    }
  }

  // Alternate between even and odd set bits, checking each time
  for (uint32_t i = 0; i < 1000; i++) {
    EXPECT_TRUE(Verify(bv, [&](auto idx) { return i % 2 == idx % 2; }));
    bv.FlipAll();
  }
}

TEST(BitVectorTest, Any) {
  for (uint32_t size : {100, 257, 521, 1024, 1031}) {
    BitVector<> bv(size);
    EXPECT_FALSE(bv.Any());

    bv.Reset();
    EXPECT_FALSE(bv.Any());

    bv.Set(64);
    EXPECT_TRUE(bv.Any());

    bv.Reset().SetAll().FlipAll();
    EXPECT_FALSE(bv.Any());
  }
}

TEST(BitVectorTest, All) {
  for (uint32_t size : {100, 257, 521, 1024, 1031}) {
    BitVector<> bv(size);
    EXPECT_FALSE(bv.All());

    // Set "middle" bit
    bv.Set(size / 2);
    EXPECT_FALSE(bv.All());

    // Set all but last bit
    for (uint32_t i = 0; i < bv.GetNumBits() - 1; i++) {
      bv.Set(i);
    }
    EXPECT_FALSE(bv.All());

    // Now, set last manually
    bv.Set(bv.GetNumBits() - 1);
    EXPECT_TRUE(bv.All());

    bv.Reset().SetAll();
    EXPECT_TRUE(bv.All());
  }
}

TEST(BitVectorTest, None) {
  BitVector<> bv(1024);
  EXPECT_TRUE(bv.None());

  bv.Reset();
  EXPECT_TRUE(bv.None());

  bv.Set(64);
  EXPECT_FALSE(bv.None());

  bv.Unset(64);
  EXPECT_TRUE(bv.None());

  bv.SetAll();
  EXPECT_FALSE(bv.None());

  bv.Reset();
  EXPECT_TRUE(bv.None());
}

TEST(BitVectorTest, SetFromBytes) {
  // Simple
  {
    BitVector<> bv(10);

    // Set first last bit only
    bv.SetFromBytes(std::vector<uint8_t>{0xff, 0, 0, 0, 0, 0, 0, 0, 0, 0xff}.data(), 10);
    EXPECT_TRUE(Verify(bv, {0, 9}));

    // Set odd bits
    bv.SetFromBytes(std::vector<uint8_t>{0, 0xff, 0, 0xff, 0, 0xff, 0, 0xff, 0, 0xff}.data(), 10);
    EXPECT_TRUE(Verify(bv, {1, 3, 5, 7, 9}));
  }

  // Complex
  {
    // Use a non-multiple of the vector size to force execution of the tail
    // process loop.
    constexpr uint32_t vec_size = common::Constants::K_DEFAULT_VECTOR_SIZE + 101 /* prime */;
    BitVector<> bv(vec_size);

    // Set even indexes
    std::random_device r;
    alignas(16) uint8_t bytes[vec_size] = {0};
    uint32_t num_set = 0;
    for (auto &byte : bytes) {
      byte = -static_cast<int>(r() % 4 == 0);
      num_set += static_cast<unsigned int>(byte == 0xff);
    }

    // Check only even indexes set
    bv.SetFromBytes(bytes, vec_size);
    EXPECT_TRUE(Verify(bv, [&](uint32_t idx) { return bytes[idx] == 0xff; }));
    EXPECT_EQ(num_set, bv.CountOnes());
  }
}

TEST(BitVectorTest, NthOne) {
  {
    BitVector<> bv = Make({0, 0, 1, 0, 0, 1, 0, 1, 1, 0, 1, 0});
    EXPECT_EQ(2u, bv.NthOne(0));
    EXPECT_EQ(5u, bv.NthOne(1));
    EXPECT_EQ(7u, bv.NthOne(2));
    EXPECT_EQ(8u, bv.NthOne(3));
    EXPECT_EQ(10u, bv.NthOne(4));
    EXPECT_EQ(bv.GetNumBits(), bv.NthOne(10));
  }

  // Multi-word
  {
    BitVector<> bv(140);
    EXPECT_EQ(bv.GetNumBits(), bv.NthOne(0));

    bv.Set(7);
    bv.Set(71);
    bv.Set(131);

    EXPECT_EQ(7u, bv.NthOne(0));
    EXPECT_EQ(71u, bv.NthOne(1));
    EXPECT_EQ(131u, bv.NthOne(2));
  }
}

TEST(BitVectorTest, Intersect) {
  BitVector<> bv1 = Make({0, 0, 1, 0, 0, 1, 0, 1, 1, 0, 1, 0});
  BitVector<> bv2 = Make({0, 1, 0, 0, 0, 1, 0, 1, 0, 0, 1, 0});
  bv1.Intersect(bv2);
  EXPECT_TRUE(Verify(bv1, {5, 7, 10}));
}

TEST(BitVectorTest, AssignmentIntersect) {
  BitVector<> bv1(100), bv2(100);
  bv1.SetRange(24, 80);
  bv2.SetRange(10, 30);
  bv1 &= bv2;
  EXPECT_TRUE(Verify(bv1, {24, 25, 26, 27, 28, 29}));
}

TEST(BitVectorTest, NonMemberIntersect) {
  BitVector<> bv1(100), bv2(100);
  bv1.SetRange(60, 100);
  bv2.SetRange(20, 70);
  BitVector<> result = bv1 & bv2;
  EXPECT_TRUE(Verify(result, {60, 61, 62, 63, 64, 65, 66, 67, 68, 69}));
}

TEST(BitVectorTest, Union) {
  BitVector<> bv1 = Make({0, 0, 1, 0, 0, 1, 0, 1, 1, 0, 1, 0});
  BitVector<> bv2 = Make({0, 1, 0, 0, 0, 1, 0, 1, 0, 0, 1, 0});
  bv1.Union(bv2);
  EXPECT_TRUE(Verify(bv1, {1, 2, 5, 7, 8, 10}));
}

TEST(BitVectorTest, Difference) {
  BitVector<> bv1 = Make({0, 0, 1, 0, 0, 1, 0, 1, 1, 0, 1, 0});
  BitVector<> bv2 = Make({0, 1, 0, 0, 0, 1, 0, 1, 0, 0, 1, 0});
  bv1.Difference(bv2);
  EXPECT_TRUE(Verify(bv1, {2, 8}));
}

TEST(BitVectorTest, XOR) {
  BitVector<> bv1 = Make({0, 0, 1, 0, 0, 1, 0, 1, 1, 0, 1, 0});
  BitVector<> bv2 = Make({0, 1, 0, 0, 0, 1, 0, 1, 0, 0, 1, 0});
  bv1.Xor(bv2);
  EXPECT_TRUE(Verify(bv1, {1, 2, 8}));

  auto bv3 = bv1 ^ bv1;  // XORing same inputs is zero.
  EXPECT_TRUE(Verify(bv3, {}));
}

TEST(BitVectorTest, ResizeSmaller) {
  BitVector<> bv(70);
  bv.SetAll();
  EXPECT_TRUE(bv.All());

  bv.Resize(40);
  EXPECT_EQ(40u, bv.GetNumBits());
  EXPECT_EQ(1u, bv.GetNumWords());

  // Bits [0, 70) were set before the resize, bits [0, 40) should be unchanged after, too.
  EXPECT_TRUE(bv.All());

  bv.Reset();
  bv.SetRange(15, 20);
  EXPECT_TRUE(Verify(bv, [](auto idx) { return idx >= 15 && idx < 20; }));
}

TEST(BitVectorTest, ResizeLarger) {
  BitVector<> bv(10);
  bv.SetAll();
  EXPECT_TRUE(bv.All());

  bv.Resize(70);
  EXPECT_EQ(70u, bv.GetNumBits());
  EXPECT_EQ(2u, bv.GetNumWords());

  // Bits [0, 10) were set before the resize, they should still be set after resize. But, [10, 70)
  // should be zero.
  EXPECT_FALSE(bv.All());
  EXPECT_TRUE(bv.Any());
  EXPECT_TRUE(Verify(bv, [](auto idx) { return idx < 10; }));

  bv.Reset();
  bv.SetRange(63, 65);
  EXPECT_TRUE(Verify(bv, [](auto idx) { return idx == 63 || idx == 64; }));
}

TEST(BitVectorTest, NoOpResize) {
  BitVector<> bv(70);
  bv.SetAll();
  EXPECT_TRUE(bv.All());

  bv.Resize(70);
  EXPECT_EQ(70u, bv.GetNumBits());
  EXPECT_EQ(2u, bv.GetNumWords());
  EXPECT_TRUE(bv.All());
}

TEST(BitVectorTest, Equality) {
  BitVector<> bv1(70);
  BitVector<> bv2(70);

  EXPECT_NE(bv1, BitVector<>(65));
  EXPECT_EQ(bv1, bv2);

  bv1[10] = true;
  EXPECT_NE(bv1, bv2);

  bv1[60] = true;
  bv2[10] = true;
  EXPECT_NE(bv1, bv2);

  bv2[60] = true;
  EXPECT_EQ(bv1, bv2);
}

TEST(BitVectorTest, Iterate) {
  // Simple
  {
    BitVector<> bv(100);
    bv.IterateSetBits([](UNUSED_ATTRIBUTE auto idx) { FAIL() << "Empty bit vectors shouldn't have any set bits"; });

    bv.Set(99);
    bv.IterateSetBits([](auto idx) { EXPECT_EQ(99u, idx); });

    bv.Set(64);
    bv.IterateSetBits([](auto idx) { EXPECT_TRUE(idx == 64 || idx == 99); });
  }

  // Complex 1
  {
    BitVector<> bv(100);
    // Set even bits
    for (uint32_t i = 0; i < bv.GetNumBits(); i++) {
      if (i % 2 == 0) {
        bv.Set(i);
      }
    }

    // Check
    bv.IterateSetBits([](auto idx) { EXPECT_EQ(0u, idx % 2); });

    // Flip and check again
    bv.FlipAll();
    bv.IterateSetBits([](auto idx) { EXPECT_NE(0u, idx % 2); });
  }
}

}  // namespace terrier::execution::util::test
